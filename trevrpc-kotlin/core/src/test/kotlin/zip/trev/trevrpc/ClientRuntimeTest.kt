package zip.trev.trevrpc

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
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

            val unary = client.unaryResponse("test.Service", "Unary", "x", strings, strings)
            assertEquals("u:x", unary.message)
            assertEquals(1, unary.metadata["result"]?.single())

            val serverStream = client.serverStreaming("test.Service", "ServerStream", "s", strings, strings)
            assertEquals(listOf("s-1"), serverStream.receiveBatch())
            assertEquals(listOf("s-2"), serverStream.receiveBatch())
            assertEquals(emptyList<String>(), serverStream.receiveBatch())
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
    fun `failed response flow does not expose success envelope metadata`() =
        runBlocking {
            val server = Server()
            server.routeServerStreaming("test.Service", "Fail") { _, _ ->
                ResponseEnvelope(
                    flow<ByteArray> { throw TrevRpcException(Status(Code.ABORTED, "stream failed")) },
                    Metadata.of("success" to byteArrayOf(1)),
                )
            }
            val call =
                Client(InMemoryTransport(server)).serverStreaming(
                    "test.Service",
                    "Fail",
                    "request",
                    strings,
                    strings,
                )

            val failure =
                try {
                    call.receive()
                    error("expected response stream failure")
                } catch (error: TrevRpcException) {
                    error
                }

            assertEquals(Code.ABORTED, failure.status.code)
            assertTrue(call.responseMetadata.isEmpty)
            server.shutdown()
        }

    @Test
    fun `streaming calls send directly and finish exactly once`() =
        runTest {
            val stream =
                RecordingClientStream(
                    RpcStreamFrame.message("reply".encodeToByteArray()),
                    RpcStreamFrame.status(Status.ok()),
                )
            val call =
                Client(SingleStreamTransport(stream))
                    .clientStreaming<String, String>("svc", "method", strings, strings)

            call.send("request")
            assertEquals(1, stream.sent.size)
            assertArrayEquals("request".encodeToByteArray(), stream.sent.single())
            call.sendBatch(listOf("batch-1", "batch-2"))
            assertEquals(1, stream.batches)
            assertEquals(listOf("request", "batch-1", "batch-2"), stream.sent.map(ByteArray::decodeToString))
            call.closeSend()
            call.closeSend()
            assertEquals(1, stream.finishes)
            assertCode(Code.CANCELLED) { call.send("late") }
            assertCode(Code.CANCELLED) { call.sendBatch(listOf("late")) }
            assertEquals("reply", call.receive().message)
            assertEquals(1, stream.closes)

            val serverStream = RecordingClientStream(RpcStreamFrame.status(Status.ok()))
            Client(SingleStreamTransport(serverStream))
                .serverStreaming("svc", "method", "request", strings, strings)
            assertEquals(1, serverStream.finishes)
        }

    @Test
    fun `request writes and half close obey the RPC deadline`() =
        runTest {
            val sendCloses = AtomicInteger()
            val blockedSend =
                SequenceStream(onClose = { sendCloses.incrementAndGet() }) { awaitCancellation() }
                    .apply { sendOperation = { awaitCancellation() } }
            val upload =
                Client(SingleStreamTransport(blockedSend))
                    .clientStreaming<String, String>(
                        "svc",
                        "method",
                        strings,
                        strings,
                        CallOptions(timeout = 1.seconds),
                    )
            assertCode(Code.DEADLINE_EXCEEDED) { upload.send("request") }
            assertEquals(1, sendCloses.get())

            val finishCloses = AtomicInteger()
            val blockedFinish =
                SequenceStream(onClose = { finishCloses.incrementAndGet() }) { awaitCancellation() }
                    .apply { finishOperation = { awaitCancellation() } }
            assertCode(Code.DEADLINE_EXCEEDED) {
                Client(SingleStreamTransport(blockedFinish))
                    .serverStreaming(
                        "svc",
                        "method",
                        "request",
                        strings,
                        strings,
                        CallOptions(timeout = 1.seconds),
                    )
            }
            assertEquals(1, finishCloses.get())
        }

    @Test
    fun `client enforces deadline idle message body and per-message limits`() =
        runTest {
            val pendingTransport =
                object : RpcTransport {
                    override suspend fun unary(request: RpcRequest): RpcResponse = awaitCancellation()

                    override suspend fun openStream(request: RpcRequest): RpcClientStream = SequenceStream { awaitCancellation() }
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

            var batchDelivered = false
            val batchedErrorStream =
                object : RpcClientStream {
                    override suspend fun send(body: ByteArray) = Unit

                    override suspend fun finishSend() = Unit

                    override suspend fun receive(): RpcStreamFrame? = null

                    override suspend fun receiveBatch(maxFrames: Int): List<RpcStreamFrame> {
                        if (batchDelivered) return emptyList()
                        batchDelivered = true
                        return listOf(
                            RpcStreamFrame.message("ok".encodeToByteArray()),
                            RpcStreamFrame.status(Status.unavailable("late failure")),
                        )
                    }

                    override suspend fun close(cause: Throwable?) = Unit
                }
            val batchedError =
                Client(SingleStreamTransport(batchedErrorStream))
                    .serverStreaming("svc", "method", "request", strings, strings)
            assertEquals(listOf("ok"), batchedError.receiveBatch())
            assertCode(Code.UNAVAILABLE) { batchedError.receiveBatch() }

            val eof = Client(StaticStreamTransport()).serverStreaming("svc", "method", "request", strings, strings)
            assertCode(Code.INTERNAL) { eof.receive() }

            val unknown =
                Client(StaticStreamTransport(RpcStreamFrame(kindValue = 99)))
                    .serverStreaming("svc", "method", "request", strings, strings)
            assertCode(Code.INVALID_ARGUMENT) { unknown.receive() }

            val unknownBatchStream =
                object : RpcClientStream {
                    override suspend fun send(body: ByteArray) = Unit

                    override suspend fun finishSend() = Unit

                    override suspend fun receive(): RpcStreamFrame? = null

                    override suspend fun receiveBatch(maxFrames: Int): List<RpcStreamFrame> = listOf(RpcStreamFrame(kindValue = 99))

                    override suspend fun close(cause: Throwable?) = Unit
                }
            val unknownBatch =
                Client(SingleStreamTransport(unknownBatchStream))
                    .serverStreaming("svc", "method", "request", strings, strings)
            assertCode(Code.INVALID_ARGUMENT) { unknownBatch.receiveBatch() }

            val trailingCloses = AtomicInteger()
            val trailingFrames =
                listOf(
                    RpcStreamFrame.status(Status.unavailable("remote")),
                    RpcStreamFrame.message("late".encodeToByteArray()),
                ).iterator()
            val trailing =
                Client(
                    SingleStreamTransport(
                        SequenceStream(onClose = { trailingCloses.incrementAndGet() }) {
                            if (trailingFrames.hasNext()) trailingFrames.next() else null
                        },
                    ),
                ).serverStreaming("svc", "method", "request", strings, strings)
            assertCode(Code.INTERNAL) { trailing.receive() }
            assertEquals(1, trailingCloses.get())

            var pendingTerminalReads = 0
            val pendingTerminal =
                Client(
                    SingleStreamTransport(
                        SequenceStream {
                            if (pendingTerminalReads++ == 0) RpcStreamFrame.status(Status.ok()) else awaitCancellation()
                        },
                    ),
                ).serverStreaming(
                    "svc",
                    "method",
                    "request",
                    strings,
                    strings,
                    CallOptions(streamIdleTimeout = 1.milliseconds),
                )
            assertCode(Code.UNAVAILABLE) { pendingTerminal.receive() }
            assertNull(pendingTerminal.terminalStatus)

            var malformedTrailingReads = 0
            val malformedTrailing =
                Client(
                    SingleStreamTransport(
                        SequenceStream {
                            if (malformedTrailingReads++ == 0) {
                                RpcStreamFrame.status(Status.ok())
                            } else {
                                throw TrevRpcException(Status.internal("malformed trailing protobuf"))
                            }
                        },
                    ),
                ).serverStreaming("svc", "method", "request", strings, strings)
            assertStatus(Code.INTERNAL, "response stream contained data after final status") {
                malformedTrailing.receive()
            }
            assertNull(malformedTrailing.terminalStatus)

            val multiple =
                Client(
                    StaticStreamTransport(
                        RpcStreamFrame.message("one".encodeToByteArray()),
                        RpcStreamFrame.message("two".encodeToByteArray()),
                        RpcStreamFrame.status(Status.ok()),
                    ),
                ).clientStreaming<String, String>("svc", "method", strings, strings)
            assertStatus(
                Code.INTERNAL,
                "client-streaming RPC returned more than one response message",
            ) { multiple.receive() }

            val multipleWithoutStatus =
                Client(
                    StaticStreamTransport(
                        RpcStreamFrame.message("one".encodeToByteArray()),
                        RpcStreamFrame.message("two".encodeToByteArray()),
                    ),
                ).clientStreaming<String, String>("svc", "method", strings, strings)
            assertStatus(Code.INTERNAL, "response stream ended before final status") {
                multipleWithoutStatus.receive()
            }

            val multipleThenRemote =
                Client(
                    StaticStreamTransport(
                        RpcStreamFrame.message("one".encodeToByteArray()),
                        RpcStreamFrame.message("two".encodeToByteArray()),
                        RpcStreamFrame.status(Status.unavailable("remote")),
                    ),
                ).clientStreaming<String, String>("svc", "method", strings, strings)
            assertStatus(Code.UNAVAILABLE, "remote") { multipleThenRemote.receive() }

            val multipleThenTrailing =
                Client(
                    StaticStreamTransport(
                        RpcStreamFrame.message("one".encodeToByteArray()),
                        RpcStreamFrame.message("two".encodeToByteArray()),
                        RpcStreamFrame.status(Status.ok()),
                        RpcStreamFrame.message("late".encodeToByteArray()),
                    ),
                ).clientStreaming<String, String>("svc", "method", strings, strings)
            assertStatus(Code.INTERNAL, "response stream contained data after final status") {
                multipleThenTrailing.receive()
            }

            val oneThenRemote =
                Client(
                    StaticStreamTransport(
                        RpcStreamFrame.message("one".encodeToByteArray()),
                        RpcStreamFrame.status(Status.unavailable("remote")),
                    ),
                ).clientStreaming<String, String>("svc", "method", strings, strings)
            assertCode(Code.UNAVAILABLE) { oneThenRemote.receive() }

            val messageWithoutStatus =
                Client(StaticStreamTransport(RpcStreamFrame.message("one".encodeToByteArray())))
                    .clientStreaming<String, String>("svc", "method", strings, strings)
            assertCode(Code.INTERNAL) { messageWithoutStatus.receive() }

            val remote =
                Client(
                    object : RpcTransport {
                        override suspend fun unary(request: RpcRequest): RpcResponse =
                            RpcResponse(
                                statusValue = Code.UNAVAILABLE.value,
                                message = "remote down",
                                body = ByteArray(1024),
                            )

                        override suspend fun openStream(request: RpcRequest): RpcClientStream = StaticStreamTransport().openStream(request)
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

                    override suspend fun openStream(request: RpcRequest): RpcClientStream =
                        SequenceStream(onClose = { closes.incrementAndGet() }) { awaitCancellation() }
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
                object : RpcClientStream {
                    override suspend fun send(body: ByteArray) = Unit

                    override suspend fun finishSend() = Unit

                    override suspend fun receive(): RpcStreamFrame = RpcStreamFrame.status(Status.unavailable("remote down"))

                    override suspend fun close(cause: Throwable?): Unit = throw CancellationException("close cancelled")
                }
            val call =
                Client(SingleStreamTransport(stream))
                    .serverStreaming("svc", "method", "request", strings, strings)
            val error = runCatching { call.receive() }.exceptionOrNull()
            assertTrue(error is CancellationException)
            assertEquals("close cancelled", error?.message)

            val batchStream =
                object : RpcClientStream {
                    override suspend fun send(body: ByteArray) = Unit

                    override suspend fun finishSend() = Unit

                    override suspend fun receive(): RpcStreamFrame? = null

                    override suspend fun receiveBatch(maxFrames: Int): List<RpcStreamFrame> =
                        listOf(
                            RpcStreamFrame.message("response".encodeToByteArray()),
                            RpcStreamFrame.status(Status.ok()),
                        )

                    override suspend fun close(cause: Throwable?): Unit = throw CancellationException("batch close cancelled")
                }
            val batchCall =
                Client(SingleStreamTransport(batchStream))
                    .serverStreaming("svc", "method", "request", strings, strings)
            val batchError = runCatching { batchCall.receiveBatch() }.exceptionOrNull()
            assertTrue(batchError is CancellationException)
            assertEquals("batch close cancelled", batchError?.message)
        }

    private suspend fun assertCode(
        code: Code,
        block: suspend () -> Unit,
    ) {
        assertEquals(code, captureTrevRpcException(code, block).status.code)
    }

    private suspend fun assertStatus(
        code: Code,
        message: String,
        block: suspend () -> Unit,
    ) {
        val error = captureTrevRpcException(code, block)
        assertEquals(code, error.status.code)
        assertEquals(message, error.status.message)
    }

    private suspend fun captureTrevRpcException(
        code: Code,
        block: suspend () -> Unit,
    ): TrevRpcException =
        try {
            block()
            throw AssertionError("expected TrevRpcException with code $code")
        } catch (error: TrevRpcException) {
            error
        }
}

private class InMemoryTransport(
    private val server: Server,
) : RpcTransport {
    override suspend fun unary(request: RpcRequest): RpcResponse = server.handleUnary(request)

    override suspend fun openStream(request: RpcRequest): RpcClientStream {
        val requests = Channel<ByteArray>(1)
        val responses = server.handleStreaming(request, requests.receiveAsFlow())
        return object : RpcClientStream {
            override suspend fun send(body: ByteArray) {
                requests.send(body.copyOf())
            }

            override suspend fun finishSend() {
                requests.close()
            }

            override suspend fun receive(): RpcStreamFrame? = responses.receive()

            override suspend fun close(cause: Throwable?) {
                requests.cancel(CancellationException("in-memory request stream closed", cause))
                responses.close(cause)
            }
        }
    }
}

private class StaticStreamTransport(
    vararg frames: RpcStreamFrame,
) : RpcTransport {
    private val frames = frames.toList()

    override suspend fun unary(request: RpcRequest): RpcResponse = RpcResponse.fromStatus(Status.unimplemented("unary"))

    override suspend fun openStream(request: RpcRequest): RpcClientStream {
        val iterator = frames.iterator()
        return SequenceStream { if (iterator.hasNext()) iterator.next() else null }
    }
}

private class SingleStreamTransport(
    private val stream: RpcClientStream,
) : RpcTransport {
    override suspend fun unary(request: RpcRequest): RpcResponse = RpcResponse.fromStatus(Status.unimplemented("unary"))

    override suspend fun openStream(request: RpcRequest): RpcClientStream = stream
}

private class SequenceStream(
    private val onClose: () -> Unit = {},
    private val next: suspend () -> RpcStreamFrame?,
) : RpcClientStream {
    var sendOperation: suspend () -> Unit = {}
    var finishOperation: suspend () -> Unit = {}

    override suspend fun send(body: ByteArray) = sendOperation()

    override suspend fun finishSend() = finishOperation()

    override suspend fun receive(): RpcStreamFrame? = next()

    override suspend fun close(cause: Throwable?) {
        onClose()
    }
}

private class RecordingClientStream(
    vararg frames: RpcStreamFrame,
) : RpcClientStream {
    private val frames = frames.iterator()
    private var sendFinished = false
    val sent = mutableListOf<ByteArray>()
    var finishes = 0
        private set
    var batches = 0
        private set
    var closes = 0
        private set

    override suspend fun send(body: ByteArray) {
        if (sendFinished) throw TrevRpcException(Status.cancelled("request stream is closed"))
        sent += body.copyOf()
    }

    override suspend fun sendBatch(bodies: List<ByteArray>) {
        if (sendFinished) throw TrevRpcException(Status.cancelled("request stream is closed"))
        batches++
        sent += bodies.map(ByteArray::copyOf)
    }

    override suspend fun finishSend() {
        if (sendFinished) return
        sendFinished = true
        finishes++
    }

    override suspend fun receive(): RpcStreamFrame? = if (frames.hasNext()) frames.next() else null

    override suspend fun close(cause: Throwable?) {
        closes++
    }
}
