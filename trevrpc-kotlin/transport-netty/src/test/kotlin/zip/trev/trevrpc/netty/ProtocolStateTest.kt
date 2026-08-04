package zip.trev.trevrpc.netty

import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import zip.trev.trevrpc.ALPN
import zip.trev.trevrpc.Code
import zip.trev.trevrpc.RpcStreamFrame
import zip.trev.trevrpc.Status
import zip.trev.trevrpc.TrevRpcException
import zip.trev.trevrpc.WireCodec
import zip.trev.trevrpc.netty.advanced.RawFrameInbox
import zip.trev.trevrpc.netty.advanced.TerminalStreamEnd
import zip.trev.trevrpc.netty.advanced.awaitTerminalStreamEnd
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds

class ProtocolStateTest {
    @Test
    fun `ALPN dispatch waits for handshake and is race idempotent`() {
        val native = AlpnDispatchState(setOf(ALPN, HTTP3_ALPN))
        assertEquals(AlpnDispatchResult.NATIVE, native.handshake(true, ALPN))
        assertEquals(AlpnDispatchResult.NATIVE, native.handshake(false, null))

        val h3 = AlpnDispatchState(setOf(ALPN, HTTP3_ALPN))
        assertEquals(AlpnDispatchResult.REJECT, h3.handshake(true, null))
        assertEquals(AlpnDispatchResult.REJECT, h3.handshake(true, HTTP3_ALPN))

        assertEquals(
            AlpnDispatchResult.HTTP3,
            AlpnDispatchState(setOf(HTTP3_ALPN)).handshake(true, HTTP3_ALPN),
        )
    }

    @Test
    fun `admitted connection cannot admit a call after synchronous shutdown entry`() {
        val gate = NettyAdmissionGate()
        val registry = NettyConnectionAdmission<String>(gate)
        val admissionChecked = CountDownLatch(1)
        val releaseAdmission = CountDownLatch(1)
        val admitted = AtomicBoolean()
        val admission =
            Thread {
                admitted.set(
                    registry.admit("racing", limit = null) {
                        admissionChecked.countDown()
                        check(releaseAdmission.await(5, TimeUnit.SECONDS))
                    },
                )
            }
        admission.start()
        assertTrue(admissionChecked.await(5, TimeUnit.SECONDS))

        val shutdownComplete = CountDownLatch(1)
        val shutdown =
            Thread {
                gate.stopAdmission()
                shutdownComplete.countDown()
            }
        shutdown.start()
        assertFalse(shutdownComplete.await(50, TimeUnit.MILLISECONDS))

        releaseAdmission.countDown()
        admission.join(5_000)
        shutdown.join(5_000)
        assertTrue(admitted.get())
        assertEquals(listOf("racing"), registry.snapshot())
        assertFalse(registry.admit("after-shutdown", limit = null))
        assertEquals(null, gate.admit { "call on admitted connection" })
    }

    @Test
    fun `combined cancellation reset completes before stream close`(): Unit =
        runBlocking {
            val channel = EmbeddedChannel()
            val reset = channel.newPromise()
            val closeIssuedAfterReset = AtomicBoolean()

            withTimeout(1.seconds) {
                awaitCancellationResetFutures(
                    issueReset = {
                        reset.setSuccess()
                        reset
                    },
                    issueClose = {
                        closeIssuedAfterReset.set(reset.isSuccess)
                        channel.newSucceededFuture()
                    },
                )
            }

            assertTrue(closeIssuedAfterReset.get())
            channel.finishAndReleaseAll()
        }

    @Test
    fun `terminal end distinguishes clean FIN missing FIN and trailing data`() =
        runBlocking {
            assertEquals(
                TerminalStreamEnd.Clean,
                awaitTerminalStreamEnd(
                    timeout = 1.seconds,
                    receive = { null },
                    missingFinMessage = "missing",
                    trailingDataMessage = "trailing",
                ),
            )

            val missing =
                TrevRpcException(
                    zip.trev.trevrpc.Status
                        .unavailable("connection lost"),
                )
            val missingEnd =
                awaitTerminalStreamEnd(
                    timeout = 1.seconds,
                    receive = { throw missing },
                    missingFinMessage = "missing",
                    trailingDataMessage = "trailing",
                )
            assertTrue(missingEnd is TerminalStreamEnd.MissingFin)
            assertEquals(missing, (missingEnd as TerminalStreamEnd.MissingFin).error)

            val trailingEnd =
                awaitTerminalStreamEnd(
                    timeout = 1.seconds,
                    receive = { byteArrayOf(1) },
                    missingFinMessage = "missing",
                    trailingDataMessage = "trailing",
                )
            assertTrue(trailingEnd is TerminalStreamEnd.TrailingData)
            assertEquals(
                "trailing",
                ((trailingEnd as TerminalStreamEnd.TrailingData).error as TrevRpcException).status.message,
            )
        }

    @Test
    fun `native partial response data after terminal status is trailing data rather than missing FIN`(): Unit =
        runBlocking {
            val options = NettyTransportOptions()
            val inbox = RawFrameInbox(options)
            val channel = EmbeddedChannel(inbox.decoder(), inbox.handler())
            val terminal = WireCodec.encode(RpcStreamFrame.status(Status.notFound("remote status")))
            val input =
                Unpooled
                    .buffer(Integer.BYTES + terminal.size + 1)
                    .writeInt(terminal.size)
                    .writeBytes(terminal)
                    .writeByte(0)

            channel.writeInbound(input)
            assertEquals(Status.notFound("remote status"), WireCodec.decodeStreamFrame(checkNotNull(inbox.receive())).status)
            channel.finish()

            val end = inbox.awaitEnd(1.seconds)
            assertTrue(end is TerminalStreamEnd.TrailingData, "unexpected terminal end: $end")
            assertEquals(Code.INTERNAL, ((end as TerminalStreamEnd.TrailingData).error as TrevRpcException).status.code)
            channel.finishAndReleaseAll()
        }

    @Test
    fun `WebTransport prelude accepts fragmentation and coalesced payload`() {
        val decoder = WebTransportPreludeDecoder(0)
        assertEquals(
            WebTransportPreludeResult.NeedMoreData,
            decoder.feed(Unpooled.wrappedBuffer(byteArrayOf(0x40))),
        )
        val result = decoder.feed(Unpooled.wrappedBuffer(byteArrayOf(0x41, 0x00, 1, 2, 3)))
        result as WebTransportPreludeResult.Accepted
        assertEquals(0, result.sessionId)
        assertArrayEquals(byteArrayOf(1, 2, 3), result.remaining)
    }

    @Test
    fun `WebTransport prelude rejects unknown malformed and early streams`() {
        val unknown = WebTransportPreludeDecoder(0).feed(Unpooled.wrappedBuffer(byteArrayOf(0x01)))
        assertTrue(unknown is WebTransportPreludeResult.Rejected)

        val longEncoding =
            WebTransportPreludeDecoder(0).feed(
                Unpooled.wrappedBuffer(byteArrayOf(0x40, 0x41, 0x00)),
            )
        assertTrue(longEncoding is WebTransportPreludeResult.Accepted)

        val wrongSession =
            WebTransportPreludeDecoder(4).feed(
                Unpooled.wrappedBuffer(byteArrayOf(0x40, 0x41, 0x00)),
            )
        assertTrue(wrongSession is WebTransportPreludeResult.Rejected)

        assertTrue(WebTransportPreludeDecoder(0).finish() is WebTransportPreludeResult.Rejected)
    }

    @Test
    fun `media type is exact single case insensitive and parameter free`() {
        assertTrue(isTrevRpcMediaType(listOf("application/trevrpc")))
        assertTrue(isTrevRpcMediaType(listOf("Application/TrevRPC")))
        assertTrue(!isTrevRpcMediaType(emptyList()))
        assertTrue(!isTrevRpcMediaType(listOf("application/trevrpc", "application/trevrpc")))
        assertTrue(!isTrevRpcMediaType(listOf("application/trevrpc; charset=utf-8")))
        assertTrue(!isTrevRpcMediaType(listOf(" application/trevrpc")))
    }

    @Test
    fun `server input overflow fails and resets exactly once`() =
        runBlocking {
            val resets = AtomicInteger()
            val input =
                ServerFrameInput(
                    NettyTransportOptions(inboundQueueCapacity = 1),
                    onOverflow = { resets.incrementAndGet() },
                )
            val frames =
                Unpooled
                    .buffer()
                    .writeInt(1)
                    .writeByte(1)
                    .writeInt(1)
                    .writeByte(2)
            try {
                input.feed(frames)
                assertArrayEquals(byteArrayOf(1), input.receive())
                val error =
                    try {
                        input.receive()
                        null
                    } catch (caught: TrevRpcException) {
                        caught
                    }
                assertEquals(Code.UNAVAILABLE, error?.status?.code)
                assertEquals(1, resets.get())

                val ignored = Unpooled.wrappedBuffer(byteArrayOf(0, 0, 0, 1, 3))
                try {
                    input.feed(ignored)
                    assertEquals(1, resets.get())
                } finally {
                    ignored.release()
                }
            } finally {
                frames.release()
            }
        }

    @Test
    fun `server input shutdown ends receive without a read-complete event`() =
        runBlocking {
            val input = ServerFrameInput(NettyTransportOptions())
            input.shutdown()
            assertEquals(null, input.receive())
        }
}
