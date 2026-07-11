package zip.trev.trevrpc.netty

import io.netty.buffer.Unpooled
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import zip.trev.trevrpc.ALPN
import zip.trev.trevrpc.Code
import zip.trev.trevrpc.TrevRpcException
import java.util.concurrent.atomic.AtomicInteger

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
            val input = ServerFrameInput(NettyTransportOptions(inboundQueueCapacity = 1)) { resets.incrementAndGet() }
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
}
