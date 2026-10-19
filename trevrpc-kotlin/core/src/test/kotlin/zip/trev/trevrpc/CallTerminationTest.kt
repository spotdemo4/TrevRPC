package zip.trev.trevrpc

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * The call-termination rules of the wire protocol, checked against fake transports whose request
 * side never drains (the server stopped reading but did not stop the request side) and whose
 * response side is controlled by the test.
 */
class CallTerminationTest {
    private val strings = MessageCodec<String>(String::encodeToByteArray, ByteArray::decodeToString)
    private val patience = 5.seconds

    private fun <T> bounded(block: suspend kotlinx.coroutines.CoroutineScope.() -> T): T = runBlocking { withTimeout(patience) { block() } }

    @Test
    fun `a non-OK status fails a blocked client-streaming send with that status`() =
        bounded {
            val stream = ScriptedStream()
            val call =
                Client(OneStream(stream))
                    .clientStreaming<String, String>("/svc/m", strings, strings, CallOptions(timeout = 1.hours))
            val blocked = async { runCatching { call.send("a") } }
            stream.awaitSend()

            stream.incoming.send(status(Status(Code.ABORTED, "stop")))
            stream.incoming.close()

            val error = blocked.await().exceptionOrNull() as TrevRpcException
            assertEquals(Code.ABORTED, error.status.code)
            assertEquals("stop", error.status.message)
            // Later sends fail the same way, without touching the transport again.
            val sends = stream.sends.get()
            val later = runCatching { call.send("b") }.exceptionOrNull() as TrevRpcException
            assertEquals(Code.ABORTED, later.status.code)
            assertEquals(sends, stream.sends.get())
            // The same status is the call's outcome.
            val outcome = runCatching { call.receive() }.exceptionOrNull() as TrevRpcException
            assertEquals(Code.ABORTED, outcome.status.code)
        }

    @Test
    fun `a non-OK status behind unread responses fails sends of a bidi call that is not receiving`() =
        bounded {
            val stream = ScriptedStream()
            repeat(20) { stream.incoming.send(message("m$it")) }
            stream.incoming.send(status(Status(Code.NOT_FOUND, "gone")))
            stream.incoming.close()
            val call =
                Client(OneStream(stream))
                    .bidirectionalStreaming<String, String>("/svc/m", strings, strings, CallOptions(timeout = 1.hours))

            val error = runCatching { call.send("a") }.exceptionOrNull() as TrevRpcException
            assertEquals(Code.NOT_FOUND, error.status.code)
            // The caller still receives everything the server sent, then the status.
            repeat(20) { assertEquals("m$it", call.receive()) }
            val outcome = runCatching { call.receive() }.exceptionOrNull() as TrevRpcException
            assertEquals(Code.NOT_FOUND, outcome.status.code)
        }

    @Test
    fun `an early OK status ends sends with Cancelled and the response is still received`() =
        bounded {
            val stream = ScriptedStream()
            val call =
                Client(OneStream(stream))
                    .clientStreaming<String, String>("/svc/m", strings, strings, CallOptions(timeout = 1.hours))
            val blocked = async { runCatching { call.send("a") } }
            stream.awaitSend()

            stream.incoming.send(message("early"))
            stream.incoming.send(status(Status.ok()))
            stream.incoming.close()

            val error = blocked.await().exceptionOrNull() as TrevRpcException
            assertEquals(Code.CANCELLED, error.status.code)
            assertEquals("early", call.receive().message)
        }

    @Test
    fun `an early OK status ends the sends of a bidi call and receive completes normally`() =
        bounded {
            val stream = ScriptedStream()
            val call = Client(OneStream(stream)).bidirectionalStreaming<String, String>("/svc/m", strings, strings)
            val blocked = async { runCatching { call.send("a") } }
            stream.awaitSend()

            stream.incoming.send(message("one"))
            stream.incoming.send(status(Status.ok()))
            stream.incoming.close()

            val error = blocked.await().exceptionOrNull() as TrevRpcException
            assertEquals(Code.CANCELLED, error.status.code)
            assertEquals("one", call.receive())
            assertNull(call.receive())
            assertEquals(Code.OK, call.terminalStatus?.code)
        }

    @Test
    fun `a response transport error after the upload stopped is what send reports`() =
        bounded {
            // The transport refuses writes because the connection is gone; the response side says why.
            val refusing =
                ScriptedStream().apply {
                    sendBehavior = { throw IllegalStateException("stream is closed") }
                    incoming.close(TrevRpcException(Status.unavailable("connection lost")))
                }
            val call =
                Client(OneStream(refusing))
                    .clientStreaming<String, String>("/svc/m", strings, strings, CallOptions(timeout = 1.hours))
            val error = runCatching { call.send("a") }.exceptionOrNull() as TrevRpcException
            assertEquals(Code.UNAVAILABLE, error.status.code)
            assertEquals("connection lost", error.status.message)

            // Blocked send: the response fails while the write waits.
            val blockedStream = ScriptedStream()
            val blockedCall =
                Client(OneStream(blockedStream))
                    .bidirectionalStreaming<String, String>("/svc/m", strings, strings, CallOptions(timeout = 1.hours))
            val blocked = async { runCatching { blockedCall.send("a") } }
            blockedStream.awaitSend()
            blockedStream.incoming.close(TrevRpcException(Status.unavailable("reset by peer")))
            val blockedError = blocked.await().exceptionOrNull() as TrevRpcException
            assertEquals(Code.UNAVAILABLE, blockedError.status.code)
            assertEquals("reset by peer", blockedError.status.message)
            // The same failure is what receive reports.
            val outcome = runCatching { blockedCall.receive() }.exceptionOrNull() as TrevRpcException
            assertEquals("reset by peer", outcome.status.message)
        }

    @Test
    fun `a write failure with no explanation from the response is reported as is`() =
        bounded {
            val stream =
                ScriptedStream().apply { sendBehavior = { throw TrevRpcException(Status.invalidArgument("bad message")) } }
            val call =
                Client(OneStream(stream))
                    .clientStreaming<String, String>("/svc/m", strings, strings, CallOptions(streamIdleTimeout = 200.milliseconds))
            val error = runCatching { call.send("a") }.exceptionOrNull() as TrevRpcException
            assertEquals(Code.INVALID_ARGUMENT, error.status.code)
            assertEquals(1, stream.closes.get())
        }

    @Test
    fun `finding the status reads ahead only as far as the call's response limits allow`() =
        bounded {
            val stream = ScriptedStream()
            repeat(100) { stream.incoming.send(message("m$it")) }
            val call =
                Client(OneStream(stream))
                    .bidirectionalStreaming<String, String>(
                        "/svc/m",
                        strings,
                        strings,
                        CallOptions(timeout = 1.hours, maxResponseMessages = 3),
                    )
            val error = runCatching { call.send("a") }.exceptionOrNull() as TrevRpcException
            assertEquals(Code.RESOURCE_EXHAUSTED, error.status.code)
            // At most the limit plus the frame that exceeds it was read.
            assertTrue(stream.receives.get() <= 5, "read ${stream.receives.get()} frames")
            repeat(3) { assertEquals("m$it", call.receive()) }
            val outcome = runCatching { call.receive() }.exceptionOrNull() as TrevRpcException
            assertEquals(Code.RESOURCE_EXHAUSTED, outcome.status.code)
        }

    @Test
    fun `read ahead without a limit stops at a documented bound and close releases the pump`() =
        bounded {
            val stream = ScriptedStream()
            repeat(PUMP_FALLBACK_MAX_MESSAGES + 500) { stream.incoming.send(message("m")) }
            val call =
                Client(OneStream(stream))
                    .bidirectionalStreaming<String, String>(
                        "/svc/m",
                        strings,
                        strings,
                        CallOptions(timeout = 1.hours, maxResponseMessages = null),
                    )
            val blocked = async { runCatching { call.send("a") } }
            stream.awaitSend()
            delay(200.milliseconds)
            assertTrue(!blocked.isCompleted)
            assertTrue(stream.receives.get() <= PUMP_FALLBACK_MAX_MESSAGES + 2, "read ${stream.receives.get()} frames")

            call.close()

            val error = blocked.await().exceptionOrNull() as TrevRpcException
            assertEquals(Code.CANCELLED, error.status.code)
            assertEquals(1, stream.closes.get())
        }

    @Test
    fun `the pump stops reading at the status and close cancels a pending read`() =
        bounded {
            val finished = ScriptedStream()
            finished.incoming.send(message("r"))
            finished.incoming.send(status(Status.ok()))
            finished.incoming.close()
            val call = Client(OneStream(finished)).clientStreaming<String, String>("/svc/m", strings, strings)
            call.closeSend()
            assertEquals("r", call.receive().message)
            assertEquals(1, finished.closes.get())
            // message, status, and the one read that proves the end of the stream.
            assertEquals(3, finished.receives.get())

            val pending = ScriptedStream()
            val open = Client(OneStream(pending)).bidirectionalStreaming<String, String>("/svc/m", strings, strings)
            pending.awaitReceive()
            open.close()
            pending.receiveCancelled.await()
            assertEquals(1, pending.closes.get())
        }

    @Test
    fun `sendAll uploads every request and half-closes`() =
        bounded {
            val stream = ScriptedStream().apply { sendBehavior = {} }
            val call = Client(OneStream(stream)).clientStreaming<String, String>("/svc/m", strings, strings)
            call.sendAll(flowOf("a", "b", "c"))
            assertEquals(3, stream.sends.get())
            assertEquals(1, stream.finishes.get())
            // A second half-close is a no-op.
            call.closeSend()
            assertEquals(1, stream.finishes.get())
        }

    @Test
    fun `sendAll stops quietly when the server ends the call and receive reports the outcome`() =
        bounded {
            val ok = ScriptedStream()
            val okCall = Client(OneStream(ok)).clientStreaming<String, String>("/svc/m", strings, strings)
            val okUpload = async { runCatching { okCall.sendAll(flow { while (true) emit("x") }) } }
            ok.awaitSend()
            ok.incoming.send(message("answer"))
            ok.incoming.send(status(Status.ok()))
            ok.incoming.close()
            assertTrue(okUpload.await().isSuccess)
            assertEquals("answer", okCall.receive().message)

            val failed = ScriptedStream()
            val failedCall = Client(OneStream(failed)).bidirectionalStreaming<String, String>("/svc/m", strings, strings)
            val failedUpload = async { runCatching { failedCall.sendAll(flow { while (true) emit("x") }) } }
            failed.awaitSend()
            failed.incoming.send(message("before"))
            failed.incoming.send(status(Status(Code.ABORTED, "stop")))
            failed.incoming.close()
            assertTrue(failedUpload.await().isSuccess)
            assertEquals("before", failedCall.receive())
            val outcome = runCatching { failedCall.receive() }.exceptionOrNull() as TrevRpcException
            assertEquals(Code.ABORTED, outcome.status.code)
        }

    @Test
    fun `sendAll rethrows failures that do not come from the response`() =
        bounded {
            val stream = ScriptedStream().apply { sendBehavior = {} }
            val call = Client(OneStream(stream)).clientStreaming<String, String>("/svc/m", strings, strings)
            val error =
                runCatching {
                    call.sendAll(
                        flow {
                            emit("a")
                            throw IllegalStateException("source failed")
                        },
                    )
                }.exceptionOrNull()
            assertEquals("source failed", error?.message)
        }

    @Test
    fun `the server's status wins when the call deadline cuts the wait for the end of the stream short`() =
        bounded {
            fun hangingAfter(vararg frames: ResponseFrame): RpcTransport {
                val reads = AtomicInteger()
                return OneStream(
                    object : RpcClientStream {
                        override suspend fun send(body: ByteArray) = Unit

                        override suspend fun finishSend() = Unit

                        override suspend fun receive(): ResponseFrame? {
                            val index = reads.getAndIncrement()
                            if (index < frames.size) return frames[index]
                            awaitCancellation()
                        }

                        override suspend fun close(cause: Throwable?) = Unit
                    },
                )
            }
            val options = CallOptions(timeout = 300.milliseconds)

            val streaming =
                Client(hangingAfter(message("a"), status(Status.ok())))
                    .serverStreaming("/svc/m", "request", strings, strings, options)
            assertEquals("a", streaming.receive())
            assertNull(streaming.receive())
            assertEquals(Code.OK, streaming.terminalStatus?.code)

            val unary =
                Client(hangingAfter(message("a"), status(Status.ok())))
                    .unaryResponse("/svc/m", "request", strings, strings, options)
            assertEquals("a", unary.message)

            val failed =
                Client(hangingAfter(status(Status(Code.ABORTED, "stop"))))
                    .serverStreaming("/svc/m", "request", strings, strings, options)
            val error = runCatching { failed.receive() }.exceptionOrNull() as TrevRpcException
            assertEquals(Code.ABORTED, error.status.code)
            assertEquals(Code.ABORTED, failed.terminalStatus?.code)

            val upload =
                Client(hangingAfter(message("a"), status(Status.ok())))
                    .clientStreaming<String, String>("/svc/m", strings, strings, options)
            upload.closeSend()
            assertEquals("a", upload.receive().message)
        }

    @Test
    fun `cancelling the caller while waiting for the end of the stream keeps the server status`() =
        bounded {
            val waiting = CompletableDeferred<Unit>()
            val closes = AtomicInteger()
            val reads = AtomicInteger()
            val transport =
                OneStream(
                    object : RpcClientStream {
                        override suspend fun send(body: ByteArray) = Unit

                        override suspend fun finishSend() = Unit

                        override suspend fun receive(): ResponseFrame? {
                            if (reads.getAndIncrement() == 0) return status(Status(Code.ABORTED, "stop"))
                            waiting.complete(Unit)
                            awaitCancellation()
                        }

                        override suspend fun close(cause: Throwable?) {
                            closes.incrementAndGet()
                        }
                    },
                )
            val call = Client(transport).serverStreaming("/svc/m", "request", strings, strings)
            val receiving = launch { call.receive() }
            waiting.await()

            receiving.cancelAndJoin()

            assertEquals(Code.ABORTED, call.terminalStatus?.code)
            assertEquals(1, closes.get())
            assertTrue(receiving.isCancelled)
            assertTrue(runCatching { call.receive() }.exceptionOrNull() !is CancellationException)
        }
}

private fun message(text: String): ResponseFrame = ResponseFrame.Message(text.encodeToByteArray())

private fun status(status: Status): ResponseFrame = ResponseFrame.Status(status)

private class OneStream(
    private val stream: RpcClientStream,
) : RpcTransport {
    override suspend fun openStream(
        request: RpcRequest,
        firstMessage: ByteArray?,
        finishSend: Boolean,
    ): RpcClientStream = stream
}

/**
 * A stream whose request side blocks until cancelled (the server is not reading) unless
 * [sendBehavior] says otherwise, and whose response side is [incoming]. Closing it does not
 * unblock a pending read, like a transport that only notices a close later.
 */
private class ScriptedStream : RpcClientStream {
    val incoming = Channel<ResponseFrame>(Channel.UNLIMITED)
    val sends = AtomicInteger()
    val receives = AtomicInteger()
    val closes = AtomicInteger()
    val finishes = AtomicInteger()
    val receiveCancelled = CompletableDeferred<Unit>()
    var sendBehavior: suspend () -> Unit = { awaitCancellation() }
    private val sendStarted = CompletableDeferred<Unit>()
    private val receiveStarted = CompletableDeferred<Unit>()

    suspend fun awaitSend() = sendStarted.await()

    suspend fun awaitReceive() = receiveStarted.await()

    override suspend fun send(body: ByteArray) {
        sends.incrementAndGet()
        sendStarted.complete(Unit)
        sendBehavior()
    }

    override suspend fun finishSend() {
        finishes.incrementAndGet()
    }

    override suspend fun receive(): ResponseFrame? {
        receives.incrementAndGet()
        receiveStarted.complete(Unit)
        try {
            val result = incoming.receiveCatching()
            result.exceptionOrNull()?.let { throw it }
            return result.getOrNull()
        } catch (error: CancellationException) {
            receiveCancelled.complete(Unit)
            throw error
        }
    }

    override suspend fun close(cause: Throwable?) {
        closes.incrementAndGet()
    }
}
