package zip.trev.trevrpc.interop

import androidx.test.platform.app.InstrumentationRegistry
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
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
            val response = client.unary(request("hello")) { copy(metadata = Metadata.of("interop-suffix" to "!".encodeToByteArray())) }
            expectEcho(response, "hello!", 0)
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
            val received = mutableListOf<EchoResponse>()
            expectStatus(Code.FAILED_PRECONDITION, "stop") {
                client.serverStream(request("s", count = 2, failCode = 9, failMessage = "stop")).collect { received += it }
            }
            expect(received.size == 2) { "expected 2 responses before the status, got ${received.size}" }
            received.forEachIndexed { index, response -> expectEcho(response, "s", index) }
        },
        "bidi" to { (client) ->
            // Each request is sent once the echo of the one before arrived.
            val messages = listOf("1", "2", "3")
            val requests = Channel<EchoRequest>(Channel.UNLIMITED)
            requests.send(request(messages[0]))
            var echoed = 0
            client.bidi(requests.consumeAsFlow()).collect { response ->
                expectEcho(response, messages[echoed], echoed)
                echoed++
                if (echoed < messages.size) requests.send(request(messages[echoed])) else requests.close()
            }
            expect(echoed == messages.size) { "expected ${messages.size} echoes, got $echoed" }
        },
        "bidi-error" to { (client) ->
            val requests = Channel<EchoRequest>(Channel.UNLIMITED)
            requests.send(request("1"))
            var echoed = 0
            expectStatus(Code.ABORTED, "abort") {
                client.bidi(requests.consumeAsFlow()).collect { response ->
                    expectEcho(response, "1", echoed)
                    echoed++
                    requests.send(request("", failCode = 10, failMessage = "abort"))
                }
            }
            expect(echoed == 1) { "expected 1 echo before the status, got $echoed" }
        },
        "unary-headers" to { (client) ->
            val response = ResponseMetadata()
            expectEcho(client.unary(request("hello")) { response.capture(withHeaderRequest()) }, "hello", 0)
            expectMetadata("headers", response.headers, "interop-header")
            expectMetadata("trailing metadata", response.trailers, "interop-trailer")
        },
        "server-stream-headers" to { (client) ->
            val response = ResponseMetadata()
            var index = 0
            client.serverStream(request("s", count = 2)) { response.capture(withHeaderRequest()) }.collect { echo ->
                // The headers arrive before the first message.
                expectMetadata("headers", response.headers, "interop-header")
                expectEcho(echo, "s", index++)
            }
            expect(index == 2) { "expected 2 responses, got $index" }
            expectMetadata("trailing metadata", response.trailers, "interop-trailer")
        },
        "unreliable" to { (_, client) ->
            // Cronet has no datagram API, so the schema's unreliable marking changes nothing: HTTP/3
            // and HTTP/2 deliver every message on the stream and the server sees no datagrams.
            val sent = (0 until 50).map { "$it" } + "x".repeat(4096)
            val response = ResponseMetadata()
            val received =
                client
                    .bidi(sent.map { request(it, count = 1) }.asFlow()) { response.capture(this) }
                    .toList()
                    .map { echo ->
                        expect(echo.index == 0) { "expected response index 0, got ${echo.index}" }
                        echo.message
                    }
            expect(received.toSet().size == received.size) { "a response was delivered more than once" }
            expect(sent.containsAll(received)) { "a response was not one that was sent" }
            expect(received.size == sent.size) { "expected ${sent.size} responses, got ${received.size}" }
            expect(sent.last() in received) { "the 4096-byte echo is missing" }
            expectCounter(response.trailers, "interop-received", sent.size.toString())
            expectCounter(response.trailers, "interop-datagrams-received", "0")
        },
    )

/** The response headers and trailing metadata of one call, reported through its options. */
private class ResponseMetadata {
    @Volatile
    private var headersOrNull: Metadata? = null

    @Volatile
    private var trailersOrNull: Metadata? = null

    val headers: Metadata
        get() = headersOrNull ?: throw AssertionError("no response headers were reported")

    val trailers: Metadata
        get() = trailersOrNull ?: throw AssertionError("no trailing metadata was reported")

    fun capture(options: CallOptions): CallOptions =
        options.copy(onResponseHeaders = { headersOrNull = it }, onResponseMetadata = { trailersOrNull = it })
}

private fun expectCounter(
    metadata: Metadata,
    key: String,
    expected: String,
) {
    val actual = metadata[key]?.decodeToString()
    expect(actual == expected) { "expected trailing metadata $key=$expected, got $actual" }
}

private fun CallOptions.withHeaderRequest(): CallOptions = copy(metadata = Metadata.of("interop-header" to "h".encodeToByteArray()))

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
