package zip.trev.trevrpc.interop

import androidx.test.platform.app.InstrumentationRegistry
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.chromium.net.CronetEngine
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import trevrpc.interop.v1.EchoClient
import trevrpc.interop.v1.Interop.EchoRequest
import trevrpc.interop.v1.Interop.EchoResponse
import trevrpc.interop.v1.UnreliableEchoClient
import zip.trev.trevrpc.CallOptions
import zip.trev.trevrpc.Code
import zip.trev.trevrpc.Metadata
import zip.trev.trevrpc.RpcChannel
import zip.trev.trevrpc.TrevRpcException
import zip.trev.trevrpc.cronet.CronetRpcChannel
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.seconds

/**
 * Runs the scenarios from interop/README.md against the host's interop server over ordinary
 * HTTP/3. Each scenario is one test case named `scenario[<name>]`, which the runner reports as
 * `ok <name>` or `fail <name>: <reason>`.
 */
@RunWith(Parameterized::class)
class EchoScenarioTest(
    private val name: String,
) {
    @Test(timeout = 60_000)
    fun scenario() {
        val body = checkNotNull(SCENARIOS[name]) { "unknown scenario $name" }
        val options = CallOptions(timeout = 10.seconds)
        runBlocking {
            withTimeout(30.seconds) { body(Clients(EchoClient(channel, options), UnreliableEchoClient(channel, options))) }
        }
    }

    companion object {
        private lateinit var engine: CronetEngine
        private lateinit var executor: ExecutorService
        private lateinit var channel: RpcChannel

        @JvmStatic
        @Parameterized.Parameters(name = "{0}")
        fun names(): List<String> = SCENARIOS.keys.toList()

        @JvmStatic
        @BeforeClass
        fun connect() {
            val arguments = InstrumentationRegistry.getArguments()
            val host = requireNotNull(arguments.getString("host")) { "the host argument is required" }
            val port = requireNotNull(arguments.getString("port")?.toIntOrNull()) { "the port argument is required" }
            // "http2" disables QUIC, as on a network that blocks UDP, so Cronet uses the server's
            // TCP listener; otherwise a QUIC hint makes it use HTTP/3 from the first request.
            val http3 = arguments.getString("transport", "http3") != "http2"
            val builder =
                CronetEngine
                    .Builder(InstrumentationRegistry.getInstrumentation().targetContext)
                    .enableQuic(http3)
            if (http3) {
                builder.addQuicHint(host, port, port)
            }
            engine = builder.build()
            executor = Executors.newFixedThreadPool(2)
            channel = CronetRpcChannel.create(engine, "https://$host:$port", executor)
        }

        @JvmStatic
        @AfterClass
        fun disconnect() {
            // Close the channel before releasing the engine and executor it borrows.
            runBlocking { withTimeout(10.seconds) { channel.close() } }
            engine.shutdown()
            executor.shutdown()
            check(executor.awaitTermination(10, TimeUnit.SECONDS)) { "Cronet callback executor did not terminate" }
        }
    }
}

/** The generated clients a scenario can call; scenarios destructure the ones they use. */
private data class Clients(
    val echo: EchoClient,
    val unreliable: UnreliableEchoClient,
)

private val SCENARIOS: Map<String, suspend (Clients) -> Unit> =
    linkedMapOf(
        "unary" to { (client) ->
            expectEcho(client.unary(request("hello")), "hello", 0)
        },
        "unary-metadata" to { (client) ->
            val options = CallOptions(timeout = 10.seconds, metadata = Metadata.of("interop-suffix" to "!".encodeToByteArray()))
            expectEcho(client.unary(request("hello"), options), "hello!", 0)
        },
        "unary-error" to { (client) ->
            expectStatus(Code.NOT_FOUND, "missing") { client.unary(request("", failCode = 5, failMessage = "missing")) }
        },
        "unary-large" to { (client) ->
            val message = "x".repeat(1 shl 20)
            expectEcho(client.unary(request(message)), message, 0)
        },
        "client-stream" to { (client) ->
            expectEcho(client.clientStream(flowOf(request("a"), request("b"), request("c"))), "abc", 3)
        },
        "server-stream" to { (client) ->
            val responses = client.serverStream(request("s", count = 3)).toList()
            expect(responses.size == 3) { "expected 3 responses, got ${responses.size}" }
            responses.forEachIndexed { index, response -> expectEcho(response, "s", index) }
        },
        "server-stream-error" to { (client) ->
            val call = client.serverStreamResponse(request("s", count = 2, failCode = 9, failMessage = "stop"))
            try {
                expectEcho(checkNotNull(call.receive()) { "stream ended before response 0" }, "s", 0)
                expectEcho(checkNotNull(call.receive()) { "stream ended before response 1" }, "s", 1)
                expectStatus(Code.FAILED_PRECONDITION, "stop") { call.receive() }
            } finally {
                call.close()
            }
        },
        "bidi" to { (client) ->
            val call = client.bidi()
            try {
                listOf("1", "2", "3").forEachIndexed { index, message ->
                    call.send(request(message))
                    expectEcho(checkNotNull(call.receive()) { "stream ended before echo $index" }, message, index)
                }
                call.closeSend()
                expect(call.receive() == null) { "expected the stream to end with OK" }
            } finally {
                call.close()
            }
        },
        "bidi-error" to { (client) ->
            val call = client.bidi()
            try {
                call.send(request("1"))
                expectEcho(checkNotNull(call.receive()) { "stream ended before echo 0" }, "1", 0)
                try {
                    call.send(request("", failCode = 10, failMessage = "abort"))
                } catch (error: TrevRpcException) {
                    // The server may end the call before this send returns; send then reports its status.
                    expect(error.status.code == Code.ABORTED && error.status.message == "abort") {
                        "expected send to fail with ABORTED \"abort\", got ${error.status}"
                    }
                }
                expectStatus(Code.ABORTED, "abort") { call.receive() }
            } finally {
                call.close()
            }
        },
        "unary-headers" to { (client) ->
            val response = client.unaryResponse(request("hello"), headerOptions())
            expectEcho(response.message, "hello", 0)
            expectMetadata("headers", response.headers, "interop-header")
            expectMetadata("trailing metadata", response.metadata, "interop-trailer")
        },
        "server-stream-headers" to { (client) ->
            val call = client.serverStreamResponse(request("s", count = 2), headerOptions())
            try {
                expectMetadata("headers", call.headers(), "interop-header")
                expectEcho(checkNotNull(call.receive()) { "stream ended before response 0" }, "s", 0)
                expectEcho(checkNotNull(call.receive()) { "stream ended before response 1" }, "s", 1)
                expect(call.receive() == null) { "expected the stream to end with OK" }
                expectMetadata("trailing metadata", call.responseMetadata, "interop-trailer")
            } finally {
                call.close()
            }
        },
        "unreliable" to { (_, client) ->
            // Cronet has no datagram API, so the schema's unreliable marking changes nothing: HTTP/3
            // and HTTP/2 deliver every message on the stream and the server sees no datagrams.
            val sent = (0 until 50).map { "$it" } + "x".repeat(4096)
            val call = client.bidi()
            try {
                val received = mutableListOf<String>()
                coroutineScope {
                    val sender =
                        launch {
                            sent.forEach { call.send(request(it, count = 1)) }
                            call.closeSend()
                        }
                    while (true) {
                        val response = call.receive() ?: break
                        expect(response.index == 0) { "expected response index 0, got ${response.index}" }
                        received += response.message
                    }
                    sender.join()
                }
                expect(received.toSet().size == received.size) { "a response was delivered more than once" }
                expect(sent.containsAll(received)) { "a response was not one that was sent" }
                expect(received.size == sent.size) { "expected ${sent.size} responses, got ${received.size}" }
                expect(sent.last() in received) { "the 4096-byte echo is missing" }
                val trailers = call.responseMetadata
                expectCounter(trailers, "interop-received", sent.size.toString())
                expectCounter(trailers, "interop-datagrams-received", "0")
            } finally {
                call.close()
            }
        },
    )

private fun expectCounter(
    metadata: Metadata,
    key: String,
    expected: String,
) {
    val actual = metadata[key]?.decodeToString()
    expect(actual == expected) { "expected trailing metadata $key=$expected, got $actual" }
}

private fun headerOptions(): CallOptions =
    CallOptions(timeout = 10.seconds, metadata = Metadata.of("interop-header" to "h".encodeToByteArray()))

private fun expectMetadata(
    what: String,
    actual: Metadata,
    key: String,
) {
    expect(actual == Metadata.of(key to "h".encodeToByteArray())) { "expected $what {$key: h}, got $actual" }
}

private fun request(
    message: String,
    count: Int = 0,
    failCode: Int = 0,
    failMessage: String = "",
): EchoRequest =
    EchoRequest
        .newBuilder()
        .setMessage(message)
        .setCount(count)
        .setFailCode(failCode)
        .setFailMessage(failMessage)
        .build()

private fun expect(
    condition: Boolean,
    message: () -> String,
) {
    if (!condition) throw AssertionError(message())
}

private fun expectEcho(
    response: EchoResponse,
    message: String,
    index: Int,
) {
    expect(response.message == message && response.index == index) {
        "expected (${message.take(32)}, $index), got (${response.message.take(32)}, ${response.index})"
    }
}

private suspend fun expectStatus(
    code: Code,
    message: String,
    call: suspend () -> Unit,
) {
    val failure =
        try {
            call()
            null
        } catch (error: TrevRpcException) {
            error
        } ?: throw AssertionError("expected status $code \"$message\", but the call succeeded")
    expect(failure.status.code == code && failure.status.message == message) {
        "expected status $code \"$message\", got ${failure.status.code} \"${failure.status.message}\""
    }
}
