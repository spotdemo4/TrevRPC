package zip.trev.trevrpc

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds

class ClientRuntimeTest {
    private val strings = MessageCodec<String>(String::encodeToByteArray, ByteArray::decodeToString)

    @Test
    fun `all four shapes work through an in-memory bounded transport`() =
        runBlocking {
            val server = Server()
            server.routeUnary("test.Service", "Unary") { _, body ->
                ResponseEnvelope("u:${body.decodeToString()}".encodeToByteArray(), Metadata.of("result" to byteArrayOf(1)))
            }
            server.routeServerStreaming("test.Service", "ServerStream") { _, body ->
                val value = body.decodeToString()
                ResponseEnvelope(flowOf("$value-1".encodeToByteArray(), "$value-2".encodeToByteArray()))
            }
            server.routeClientStreaming("test.Service", "ClientStream") { _, requests ->
                ResponseEnvelope(requests.toList().joinToString(",") { it.decodeToString() }.encodeToByteArray())
            }
            server.routeBidirectionalStreaming("test.Service", "Bidi") { _, requests ->
                ResponseEnvelope(requests.map { "echo:${it.decodeToString()}".encodeToByteArray() })
            }
            val client = Client(InMemoryTransport(server))

            val unary = client.unaryEnvelope("test.Service", "Unary", "x", strings, strings)
            assertEquals("u:x", unary.message)
            assertEquals(1, unary.metadata["result"]?.single())

            val serverStream = client.serverStreaming("test.Service", "ServerStream", "s", strings, strings)
            assertEquals("s-1", serverStream.receive())
            assertEquals("s-2", serverStream.receive())
            assertNull(serverStream.receive())
            assertEquals(Code.OK, serverStream.terminalStatus?.code)

            val clientStream = client.clientStreaming("test.Service", "ClientStream", strings, strings)
            clientStream.send("a")
            clientStream.send("b")
            clientStream.closeSend()
            assertEquals("a,b", clientStream.receive().message)

            val bidi = client.bidirectionalStreaming("test.Service", "Bidi", strings, strings)
            bidi.send("a")
            assertEquals("echo:a", bidi.receive())
            bidi.send("b")
            bidi.closeSend()
            assertEquals("echo:b", bidi.receive())
            assertNull(bidi.receive())
            assertEquals(Code.OK, bidi.terminalStatus?.code)

            server.shutdown()
        }

    @Test
    fun `client enforces deadline idle message body and per-message limits`() =
        runTest {
            val pendingTransport =
                object : RpcTransport {
                    override suspend fun unary(request: RpcRequest): RpcResponse = awaitCancellation()

                    override suspend fun openStream(
                        request: RpcRequest,
                        requestBody: Flow<ByteArray>,
                    ): RpcTransportStream = SequenceStream { awaitCancellation() }
                }
            val client = Client(pendingTransport)
            assertCode(Code.DEADLINE_EXCEEDED) {
                client.unary(
                    "svc",
                    "method",
                    "request",
                    strings,
                    strings,
                    CallOptions(timeout = 1.seconds),
                )
            }
            val idle =
                client.serverStreaming(
                    "svc",
                    "method",
                    "request",
                    strings,
                    strings,
                    CallOptions(streamIdleTimeout = 1.seconds),
                )
            assertCode(Code.UNAVAILABLE) { idle.receive() }

            val messageLimited =
                Client(
                    StaticStreamTransport(
                        RpcStreamFrame.message("a".encodeToByteArray()),
                        RpcStreamFrame.message("b".encodeToByteArray()),
                        RpcStreamFrame.status(Status.ok()),
                    ),
                ).serverStreaming(
                    "svc",
                    "method",
                    "request",
                    strings,
                    strings,
                    CallOptions(maxResponseMessages = 1),
                )
            assertEquals("a", messageLimited.receive())
            assertCode(Code.RESOURCE_EXHAUSTED) { messageLimited.receive() }

            val bodyLimited =
                Client(
                    StaticStreamTransport(
                        RpcStreamFrame.message("ab".encodeToByteArray()),
                        RpcStreamFrame.message("c".encodeToByteArray()),
                    ),
                ).serverStreaming(
                    "svc",
                    "method",
                    "request",
                    strings,
                    strings,
                    CallOptions(maxResponseStreamBodySize = 2),
                )
            assertEquals("ab", bodyLimited.receive())
            assertCode(Code.RESOURCE_EXHAUSTED) { bodyLimited.receive() }

            val frameLimited =
                Client(
                    StaticStreamTransport(
                        RpcStreamFrame.message("ab".encodeToByteArray()),
                    ),
                ).serverStreaming(
                    "svc",
                    "method",
                    "request",
                    strings,
                    strings,
                    CallOptions(maxResponseBodySize = 1),
                )
            assertCode(Code.RESOURCE_EXHAUSTED) { frameLimited.receive() }
        }

    @Test
    fun `terminal status metadata eof malformed shape and remote precedence are enforced`() =
        runTest {
            val closes = AtomicInteger()
            val terminalFrames =
                listOf(
                    RpcStreamFrame.message("ok".encodeToByteArray()),
                    RpcStreamFrame.status(Status.ok(Metadata.of("trace" to byteArrayOf(7)))),
                ).iterator()
            val terminal =
                Client(
                    SingleStreamTransport(
                        SequenceStream(onClose = { closes.incrementAndGet() }) {
                            if (terminalFrames.hasNext()) terminalFrames.next() else null
                        },
                    ),
                ).serverStreaming("svc", "method", "request", strings, strings)
            assertEquals("ok", terminal.receive())
            assertNull(terminal.receive())
            assertEquals(7, terminal.responseMetadata["trace"]?.single())
            assertEquals(1, closes.get())

            val eof = Client(StaticStreamTransport()).serverStreaming("svc", "method", "request", strings, strings)
            assertCode(Code.INTERNAL) { eof.receive() }

            val unknown =
                Client(StaticStreamTransport(RpcStreamFrame(kindValue = 99)))
                    .serverStreaming("svc", "method", "request", strings, strings)
            assertCode(Code.INTERNAL) { unknown.receive() }

            val multiple =
                Client(
                    StaticStreamTransport(
                        RpcStreamFrame.message("one".encodeToByteArray()),
                        RpcStreamFrame.message("two".encodeToByteArray()),
                        RpcStreamFrame.status(Status.ok()),
                    ),
                ).clientStreaming<String, String>("svc", "method", strings, strings)
            assertCode(Code.INTERNAL) { multiple.receive() }

            val remote =
                Client(
                    object : RpcTransport {
                        override suspend fun unary(request: RpcRequest): RpcResponse =
                            RpcResponse(
                                statusValue = Code.UNAVAILABLE.value,
                                message = "remote down",
                                body = ByteArray(1024),
                            )

                        override suspend fun openStream(
                            request: RpcRequest,
                            requestBody: Flow<ByteArray>,
                        ): RpcTransportStream = StaticStreamTransport().openStream(request, requestBody)
                    },
                )
            assertCode(Code.UNAVAILABLE) {
                remote.unary(
                    "svc",
                    "method",
                    "request",
                    strings,
                    strings,
                    CallOptions(maxResponseBodySize = 1),
                )
            }
        }

    @Test
    fun `early and repeated close cancel transport exactly once`() =
        runTest {
            val closes = AtomicInteger()
            val transport =
                object : RpcTransport {
                    override suspend fun unary(request: RpcRequest): RpcResponse = RpcResponse.ok(byteArrayOf())

                    override suspend fun openStream(
                        request: RpcRequest,
                        requestBody: Flow<ByteArray>,
                    ): RpcTransportStream = SequenceStream(onClose = { closes.incrementAndGet() }) { awaitCancellation() }
                }
            val call = Client(transport).bidirectionalStreaming<String, String>("svc", "method", strings, strings)
            call.close()
            call.close()
            assertEquals(1, closes.get())
        }

    @Test
    fun `terminal status cleanup preserves coroutine cancellation`() =
        runTest {
            val stream =
                object : RpcTransportStream {
                    override suspend fun receive(): RpcStreamFrame = RpcStreamFrame.status(Status.unavailable("remote down"))

                    override suspend fun close(cause: Throwable?): Unit = throw CancellationException("close cancelled")
                }
            val call =
                Client(SingleStreamTransport(stream))
                    .serverStreaming("svc", "method", "request", strings, strings)
            val error = runCatching { call.receive() }.exceptionOrNull()
            assertTrue(error is CancellationException)
            assertEquals("close cancelled", error?.message)
        }

    private suspend fun assertCode(
        code: Code,
        block: suspend () -> Unit,
    ) {
        val error =
            try {
                block()
                throw AssertionError("expected TrevRpcException with code $code")
            } catch (error: TrevRpcException) {
                error
            }
        assertEquals(code, error.status.code)
    }
}

private class InMemoryTransport(
    private val server: Server,
) : RpcTransport {
    override suspend fun unary(request: RpcRequest): RpcResponse = server.handleUnary(request)

    override suspend fun openStream(
        request: RpcRequest,
        requestBody: Flow<ByteArray>,
    ): RpcTransportStream = server.handleStreaming(request, requestBody)
}

private class StaticStreamTransport(
    vararg frames: RpcStreamFrame,
) : RpcTransport {
    private val frames = frames.toList()

    override suspend fun unary(request: RpcRequest): RpcResponse = RpcResponse.fromStatus(Status.unimplemented("unary"))

    override suspend fun openStream(
        request: RpcRequest,
        requestBody: Flow<ByteArray>,
    ): RpcTransportStream {
        val iterator = frames.iterator()
        return SequenceStream { if (iterator.hasNext()) iterator.next() else null }
    }
}

private class SingleStreamTransport(
    private val stream: RpcTransportStream,
) : RpcTransport {
    override suspend fun unary(request: RpcRequest): RpcResponse = RpcResponse.fromStatus(Status.unimplemented("unary"))

    override suspend fun openStream(
        request: RpcRequest,
        requestBody: Flow<ByteArray>,
    ): RpcTransportStream = stream
}

private class SequenceStream(
    private val onClose: () -> Unit = {},
    private val next: suspend () -> RpcStreamFrame?,
) : RpcTransportStream {
    override suspend fun receive(): RpcStreamFrame? = next()

    override suspend fun close(cause: Throwable?) {
        onClose()
    }
}
