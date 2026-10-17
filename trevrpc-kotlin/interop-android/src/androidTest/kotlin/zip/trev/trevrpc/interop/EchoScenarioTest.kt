package zip.trev.trevrpc.interop

import androidx.test.platform.app.InstrumentationRegistry
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
        runBlocking { withTimeout(30.seconds) { body(EchoClient(channel, CallOptions(timeout = 10.seconds))) } }
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

private val SCENARIOS: Map<String, suspend (EchoClient) -> Unit> =
    linkedMapOf(
        "unary" to { client ->
            expectEcho(client.unary(request("hello")), "hello", 0)
        },
        "unary-metadata" to { client ->
            val options = CallOptions(timeout = 10.seconds, metadata = Metadata.of("interop-suffix" to "!".encodeToByteArray()))
            expectEcho(client.unary(request("hello"), options), "hello!", 0)
        },
        "unary-error" to { client ->
            expectStatus(Code.NOT_FOUND, "missing") { client.unary(request("", failCode = 5, failMessage = "missing")) }
        },
        "unary-large" to { client ->
            val message = "x".repeat(1 shl 20)
            expectEcho(client.unary(request(message)), message, 0)
        },
        "client-stream" to { client ->
            expectEcho(client.clientStream(flowOf(request("a"), request("b"), request("c"))), "abc", 3)
        },
        "server-stream" to { client ->
            val responses = client.serverStream(request("s", count = 3)).toList()
            expect(responses.size == 3) { "expected 3 responses, got ${responses.size}" }
            responses.forEachIndexed { index, response -> expectEcho(response, "s", index) }
        },
        "server-stream-error" to { client ->
            val call = client.serverStreamResponse(request("s", count = 2, failCode = 9, failMessage = "stop"))
            try {
                expectEcho(checkNotNull(call.receive()) { "stream ended before response 0" }, "s", 0)
                expectEcho(checkNotNull(call.receive()) { "stream ended before response 1" }, "s", 1)
                expectStatus(Code.FAILED_PRECONDITION, "stop") { call.receive() }
            } finally {
                call.close()
            }
        },
        "bidi" to { client ->
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
        "bidi-error" to { client ->
            val call = client.bidi()
            try {
                call.send(request("1"))
                expectEcho(checkNotNull(call.receive()) { "stream ended before echo 0" }, "1", 0)
                call.send(request("", failCode = 10, failMessage = "abort"))
                expectStatus(Code.ABORTED, "abort") { call.receive() }
            } finally {
                call.close()
            }
        },
    )

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
