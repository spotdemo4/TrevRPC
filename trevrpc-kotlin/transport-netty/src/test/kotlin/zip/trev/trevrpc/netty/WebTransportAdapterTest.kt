package zip.trev.trevrpc.netty

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http3.TrevRpcWebTransportServerConnectionHandler
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class WebTransportAdapterTest {
    @Test
    fun `split WebTransport varint routes raw and preserves every byte`() {
        val h3 = CaptureHandler()
        val raw = CaptureHandler()
        val channel =
            EmbeddedChannel(
                TrevRpcWebTransportServerConnectionHandler.FirstVarintDemultiplexer(h3, raw),
            )
        val first = Unpooled.wrappedBuffer(byteArrayOf(0x40))
        val second = Unpooled.wrappedBuffer(byteArrayOf(0x41, 0x00, 7, 8))

        assertFalse(channel.writeInbound(first))
        assertEquals(1, first.refCnt())
        channel.writeInbound(second)

        assertTrue(raw.addedWhileActive)
        assertEquals(0, h3.messages.size)
        assertEquals(1, raw.messages.size)
        assertArrayEquals(byteArrayOf(0x40, 0x41, 0x00, 7, 8), raw.messages.single())
        assertEquals(0, first.refCnt())
        assertEquals(0, second.refCnt())
        channel.finishAndReleaseAll()
    }

    @Test
    fun `ordinary H3 bytes bypass raw initializer unchanged`() {
        val h3 = CaptureHandler()
        val raw = CaptureHandler()
        val channel =
            EmbeddedChannel(
                TrevRpcWebTransportServerConnectionHandler.FirstVarintDemultiplexer(h3, raw),
            )
        val bytes = Unpooled.wrappedBuffer(byteArrayOf(0x01, 0x02, 0x03))

        channel.writeInbound(bytes)

        assertEquals(0, raw.messages.size)
        assertEquals(1, h3.messages.size)
        assertArrayEquals(byteArrayOf(0x01, 0x02, 0x03), h3.messages.single())
        assertEquals(0, bytes.refCnt())
        channel.finishAndReleaseAll()
    }

    @Test
    fun `incomplete first varint is released when stream closes`() {
        val bytes = Unpooled.wrappedBuffer(byteArrayOf(0x40))
        val channel =
            EmbeddedChannel(
                TrevRpcWebTransportServerConnectionHandler.FirstVarintDemultiplexer(
                    CaptureHandler(),
                    CaptureHandler(),
                ),
            )
        channel.writeInbound(bytes)
        assertEquals(1, bytes.refCnt())

        channel.finishAndReleaseAll()

        assertEquals(0, bytes.refCnt())
    }

    @Test
    fun `WebTransport settings advertise only supported extension values`() {
        val settings = TrevRpcWebTransportServerConnectionHandler.webTransportSettings().settings()
        assertEquals(true, settings.connectProtocolEnabled())
        assertEquals(true, settings.h3DatagramEnabled())
        assertEquals(1L, settings.get(TrevRpcWebTransportServerConnectionHandler.WEBTRANSPORT_DRAFT_02_SETTING))
        assertEquals(
            1L,
            settings.get(
                TrevRpcWebTransportServerConnectionHandler.WEBTRANSPORT_MAX_SESSIONS_DRAFT_07_SETTING,
            ),
        )
        assertEquals(1L, settings.get(TrevRpcWebTransportServerConnectionHandler.WEBTRANSPORT_MAX_SESSIONS_SETTING))
        val validator = TrevRpcWebTransportServerConnectionHandler.webTransportSettingsValidator()
        assertTrue(validator.validate(TrevRpcWebTransportServerConnectionHandler.WEBTRANSPORT_DRAFT_02_SETTING, 1L))
        assertFalse(validator.validate(0xdeadL, 1L))
        assertThrows(IllegalArgumentException::class.java) {
            validator.validate(TrevRpcWebTransportServerConnectionHandler.WEBTRANSPORT_DRAFT_02_SETTING, 0L)
        }
    }

    private class CaptureHandler : ChannelInboundHandlerAdapter() {
        val messages = mutableListOf<ByteArray>()
        var addedWhileActive = false

        override fun handlerAdded(context: ChannelHandlerContext) {
            addedWhileActive = context.channel().isActive
        }

        override fun channelRead(
            context: ChannelHandlerContext,
            message: Any,
        ) {
            val bytes = message as ByteBuf
            try {
                messages += bytes.copyReadableBytes()
            } finally {
                bytes.release()
            }
        }
    }
}
