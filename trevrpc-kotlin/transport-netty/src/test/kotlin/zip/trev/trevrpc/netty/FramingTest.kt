package zip.trev.trevrpc.netty

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.quic.QuicStreamFrame
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import zip.trev.trevrpc.Code
import zip.trev.trevrpc.RpcRequest
import zip.trev.trevrpc.RpcStreamFrame
import zip.trev.trevrpc.RpcStreamFrameKind
import zip.trev.trevrpc.Status
import zip.trev.trevrpc.TrevRpcException
import zip.trev.trevrpc.WireCodec
import zip.trev.trevrpc.netty.advanced.writeNativeUnaryRequest

class FramingTest {
    @Test
    fun `decodes split coalesced zero and maximum frames`() {
        val channel = EmbeddedChannel(TrevRpcFrameDecoder(4))
        val bytes =
            Unpooled.buffer().apply {
                writeInt(0)
                writeInt(4)
                writeBytes(byteArrayOf(1, 2, 3, 4))
                writeInt(1)
                writeByte(9)
            }
        val first = bytes.readRetainedSlice(3)
        val second = bytes.readRetainedSlice(4)
        val third = bytes.readRetainedSlice(bytes.readableBytes())
        bytes.release()

        channel.writeInbound(first)
        assertEquals(null, channel.readInbound<ByteBuf>())
        channel.writeInbound(second)
        val empty = channel.readInbound<ByteBuf>()
        assertEquals(0, empty.readableBytes())
        empty.release()
        channel.writeInbound(third)
        val maximum = channel.readInbound<ByteBuf>()
        val one = channel.readInbound<ByteBuf>()
        assertArrayEquals(byteArrayOf(1, 2, 3, 4), maximum.copyReadableBytes())
        assertArrayEquals(byteArrayOf(9), one.copyReadableBytes())
        maximum.release()
        one.release()
        channel.finishAndReleaseAll()
    }

    @Test
    fun `rejects oversized declaration before body allocation`() {
        val channel = EmbeddedChannel(TrevRpcFrameDecoder(4))
        val error = assertThrows(Exception::class.java) { channel.writeInbound(Unpooled.buffer(4).writeInt(5)) }
        val cause = generateSequence(error as Throwable?) { it.cause }.filterIsInstance<TrevRpcException>().first()
        assertEquals(Code.RESOURCE_EXHAUSTED, cause.status.code)
        runCatching { channel.finishAndReleaseAll() }
    }

    @Test
    fun `both partial eof paths report internal`() {
        val channel = EmbeddedChannel(TrevRpcFrameDecoder(8))
        channel.writeInbound(Unpooled.wrappedBuffer(byteArrayOf(0, 0, 0)))
        val channelError = assertThrows(Exception::class.java) { channel.finish() }
        val channelCause =
            generateSequence(channelError as Throwable?) { it.cause }
                .filterIsInstance<TrevRpcException>()
                .first()
        assertEquals(Code.INTERNAL, channelCause.status.code)
        runCatching { channel.finishAndReleaseAll() }

        val reader = IncrementalFrameReader(8)
        val partial = Unpooled.wrappedBuffer(byteArrayOf(0, 0, 0, 2, 1))
        reader.feed(partial)
        partial.release()
        val readerError = assertThrows(TrevRpcException::class.java, reader::finish)
        assertEquals(Code.INTERNAL, readerError.status.code)
    }

    @Test
    fun `emitted slices and writer buffers have explicit ownership`() {
        val input = Unpooled.buffer().writeInt(2).writeBytes(byteArrayOf(7, 8))
        val channel = EmbeddedChannel(TrevRpcFrameDecoder(8))
        channel.writeInbound(input)
        assertEquals(1, input.refCnt())
        val body = channel.readInbound<ByteBuf>()
        assertEquals(1, body.refCnt())
        body.release()
        assertEquals(0, input.refCnt())

        val framed = TrevRpcFrameWriter.encode(channel.alloc(), byteArrayOf(3, 4), 8)
        assertEquals(1, framed.refCnt())
        assertEquals(2, framed.readInt())
        assertArrayEquals(byteArrayOf(3, 4), framed.copyReadableBytes())
        framed.release()
        channel.finishAndReleaseAll()
    }

    @Test
    fun `batch writer preserves independent frame boundaries`() {
        val channel = EmbeddedChannel(TrevRpcFrameDecoder(8))
        val bodies = listOf(byteArrayOf(1, 2), byteArrayOf(), byteArrayOf(3, 4, 5))

        val encoded = TrevRpcFrameWriter.encodeBatch(channel.alloc(), bodies, 8)
        channel.writeInbound(encoded)

        bodies.forEach { expected ->
            val actual = channel.readInbound<ByteBuf>()
            assertArrayEquals(expected, actual.copyReadableBytes())
            actual.release()
        }
        assertEquals(null, channel.readInbound<ByteBuf>())
        channel.finishAndReleaseAll()
    }

    @Test
    fun `final writer combines terminal request status and QUIC FIN`() {
        val channel = EmbeddedChannel()
        val body = WireCodec.encode(RpcStreamFrame.status(Status.ok()))

        TrevRpcFrameWriter.writeFinal(channel, body, 128)

        val final = channel.readOutbound<QuicStreamFrame>()
        assertTrue(final.hasFin())
        assertEquals(body.size, final.content().readInt())
        val decoded = WireCodec.decodeStreamFrame(final.content().copyReadableBytes())
        assertEquals(RpcStreamFrameKind.STATUS, decoded.kind)
        assertEquals(Code.OK, decoded.status.code)
        final.release()
        channel.finishAndReleaseAll()
    }

    @Test
    fun `native unary request writer sends request and FIN atomically`(): Unit =
        runBlocking {
            val channel = EmbeddedChannel()
            val request = RpcRequest("test.Service", "Unary", byteArrayOf(1, 2, 3))

            writeNativeUnaryRequest(channel, request, 128)

            val final = channel.readOutbound<QuicStreamFrame>()
            assertTrue(final.hasFin())
            val encoded = ByteArray(final.content().readInt())
            final.content().readBytes(encoded)
            val decoded = WireCodec.decodeRequest(encoded)
            assertEquals(request.service, decoded.service)
            assertEquals(request.method, decoded.method)
            assertArrayEquals(request.body, decoded.body)
            final.release()
            channel.finishAndReleaseAll()
        }
}
