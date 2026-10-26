package zip.trev.trevrpc.interop

import androidx.test.platform.app.InstrumentationRegistry
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
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
import trevrpc.interop.v1.Compressed.PlainMessage
import trevrpc.interop.v1.CompressedEchoClient
import trevrpc.interop.v1.EchoClient
import trevrpc.interop.v1.Interop.EchoRequest
import trevrpc.interop.v1.Interop.EchoResponse
import trevrpc.interop.v1.RefusingEchoClient
import trevrpc.interop.v1.UnreliableEchoClient
import zip.trev.trevrpc.CallOptions
import zip.trev.trevrpc.Code
import zip.trev.trevrpc.Compression
import zip.trev.trevrpc.Dictionary
import zip.trev.trevrpc.Metadata
import zip.trev.trevrpc.RpcChannel
import zip.trev.trevrpc.TrevRpcException
import zip.trev.trevrpc.cronet.CronetRpcChannel
import zip.trev.trevrpc.cronet.CronetTransportOptions
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
            withTimeout(30.seconds) {
                body(
                    Clients(
                        EchoClient(channel, options),
                        UnreliableEchoClient(channel, options),
                        CompressedEchoClient(channel, options),
                        RefusingEchoClient(channel, options),
                        { connectionMetadata -> newChannel(connectionMetadata) },
                    ),
                )
            }
        }
    }

    companion object {
        private lateinit var engine: CronetEngine
        private lateinit var executor: ExecutorService
        private lateinit var channel: RpcChannel
        private lateinit var origin: String

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
            origin = "https://$host:$port"
            channel = newChannel()
        }

        /** A channel to the interop server on the shared engine; the caller closes it. */
        private fun newChannel(connectionMetadata: Metadata = Metadata.EMPTY): RpcChannel =
            CronetRpcChannel.create(
                engine,
                origin,
                executor,
                CronetTransportOptions(compression = interopCompression(), connectionMetadata = connectionMetadata),
            )

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

/**
 * Compression with the interop dictionary, `dict.d1` of the compression golden vectors, which the
 * runner hands the server too. It compresses the Echo messages.
 */
private fun interopCompression(): Compression {
    val dictionary = goldenVector("dict.d1.dictionary")
    return Compression(
        listOf(Dictionary(dictionary, listOf("trevrpc.interop.v1.EchoRequest", "trevrpc.interop.v1.EchoResponse"))),
    )
}

/** The value of entry [name] of the compression golden vectors. */
private fun goldenVector(name: String): ByteArray {
    val prefix = "$name = "
    val vectors = checkNotNull(Clients::class.java.classLoader?.getResourceAsStream("zstd-golden-vectors.txt"))
    val hex = vectors.bufferedReader().useLines { lines -> lines.first { it.startsWith(prefix) }.removePrefix(prefix) }
    return hex.chunked(2).map { it.toInt(16).toByte() }.toByteArray()
}

/** The text of `compressed-plain`: the `plain.records` JSON record, twice over. */
private val plainText = goldenVector("plain.records.message").decodeToString().repeat(2)

/** The generated clients a scenario can call; scenarios destructure the ones they use. */
private data class Clients(
    val echo: EchoClient,
    val unreliable: UnreliableEchoClient,
    val compressed: CompressedEchoClient,
    val refusing: RefusingEchoClient,
    /** Opens a channel of its own, with the given connection metadata, which the scenario closes. */
    val newChannel: (Metadata) -> RpcChannel,
)

/** The i-th message of the compressed scenarios: like the documents the interop dictionary was trained on. */
private fun compressedMessage(index: Int) = """{"service":"search","region":"us-east-1","status":404,"latency_ms":${100 + index}}"""

private val SCENARIOS: Map<String, suspend (Clients) -> Unit> =
    linkedMapOf(
        "unary" to { (client) ->
            expectEcho(client.unary(request("hello")), "hello", 0)
        },
        "unary-metadata" to { (client) ->
            val response = client.unary(request("hello")) { copy(metadata = Metadata.of("interop-suffix" to "!".encodeToByteArray())) }
            expectEcho(response, "hello!", 0)
        },
        "connection-metadata" to { clients ->
            val channel = clients.newChannel(suffix("!"))
            try {
                val echo = EchoClient(channel, CallOptions(timeout = 10.seconds))
                // Cronet hides which calls share a connection, so every call offers the whole set.
                for (message in listOf("a", "b", "c")) expectEcho(echo.unary(request(message)), "$message!", 0)
                // The call's own entry wins over the connection metadata's.
                expectEcho(echo.unary(request("d")) { copy(metadata = suffix("?")) }, "d?", 0)
                channel.setConnectionMetadata(suffix("#"))
                expectEcho(echo.unary(request("e")), "e#", 0)
            } finally {
                channel.close()
            }
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
        "compressed" to { (client) ->
            for (index in 0 until 2) expectEcho(client.unary(request(compressedMessage(index))), compressedMessage(index), 0)
            val sent = (0 until 3).map(::compressedMessage)
            val responses = client.bidi(sent.map { request(it) }.asFlow()).toList()
            expect(responses.size == sent.size) { "expected ${sent.size} responses, got ${responses.size}" }
            responses.forEachIndexed { index, response -> expectEcho(response, sent[index], index) }
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
        "compressed-stream" to { (_, _, client) ->
            // Requests compress optimistically from the third message, without waiting for the answer.
            expectStreamCompressed("bidi", compressedBidi(client), 6)
            expectStreamCompressed("client-stream", upload { requests, options -> client.clientStream(requests) { options(this) } }, 6)
        },
        "refused-stream" to { (_, _, client, refusing) ->
            // The server refuses the optimistic call and the client replays it, so each message
            // arrives once.
            upload { requests, options -> refusing.clientStream(requests) { options(this) } }
            // After the refusal the bidi call waits for the answer, which the server's HEADERS at
            // dispatch bring before its third message; that answer makes the client optimistic again.
            expectStreamCompressed("bidi", compressedBidi(client), 1)
            expectStreamCompressed("client-stream", upload { requests, options -> client.clientStream(requests) { options(this) } }, 6)
        },
        "compressed-plain" to { (_, _, client) ->
            repeat(2) { call ->
                val response = ResponseMetadata()
                val echo = client.plain(PlainMessage.newBuilder().setText(plainText).build()) { response.capture(this) }
                expect(echo.text == plainText) { "call $call: expected the ${plainText.length}-character text echoed" }
                // This client never compresses unary requests; the server may compress its response.
                expectCounter(response.trailers, "interop-plain-compressed-received", "0")
            }
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

/** The eight messages of the stream compression scenarios. */
private val compressedMessages = (0 until 8).map(::compressedMessage)

/**
 * `CompressedEcho.Bidi` sending [compressedMessages], each after the echo of the one before; returns
 * the trailing metadata.
 */
private suspend fun compressedBidi(client: CompressedEchoClient): Metadata {
    val requests = Channel<EchoRequest>(Channel.UNLIMITED)
    requests.send(request(compressedMessages[0]))
    val response = ResponseMetadata()
    var echoed = 0
    client.bidi(requests.consumeAsFlow()) { response.capture(this) }.collect { echo ->
        expectEcho(echo, compressedMessages[echoed], echoed)
        echoed++
        if (echoed < compressedMessages.size) requests.send(request(compressedMessages[echoed])) else requests.close()
    }
    expect(echoed == compressedMessages.size) { "expected ${compressedMessages.size} echoes, got $echoed" }
    return response.trailers
}

/**
 * A client-streaming echo of [compressedMessages], sent without waiting through [call], which gets
 * the requests and the call options to apply; checks the echo and returns the trailing metadata.
 */
private suspend fun upload(call: suspend (Flow<EchoRequest>, (CallOptions) -> CallOptions) -> EchoResponse): Metadata {
    val response = ResponseMetadata()
    val echo = call(compressedMessages.map(::request).asFlow()) { response.capture(it) }
    expectEcho(echo, compressedMessages.joinToString(""), compressedMessages.size)
    return response.trailers
}

/** Checks that the server received at least [minimum] requests of the call stream-compressed. */
private fun expectStreamCompressed(
    what: String,
    trailers: Metadata,
    minimum: Int,
) {
    val compressed = trailers["interop-stream-compressed-received"]?.decodeToString()?.toIntOrNull()
    expect(compressed != null && compressed >= minimum) {
        "$what: expected trailing metadata interop-stream-compressed-received of at least $minimum, got $compressed"
    }
}

private fun expectCounter(
    metadata: Metadata,
    key: String,
    expected: String,
) {
    val actual = metadata[key]?.decodeToString()
    expect(actual == expected) { "expected trailing metadata $key=$expected, got $actual" }
}

private fun suffix(value: String): Metadata = Metadata.of("interop-suffix" to value.encodeToByteArray())

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
