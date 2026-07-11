package zip.trev.trevrpc.netty

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufAllocator
import io.netty.channel.Channel
import io.netty.channel.ChannelFuture
import io.netty.handler.codec.ByteToMessageDecoder
import zip.trev.trevrpc.Status
import zip.trev.trevrpc.TrevRpcException

private const val FRAME_HEADER_SIZE = 4

/** Emits retained frame bodies. The next handler owns and must release every emitted [ByteBuf]. */
class TrevRpcFrameDecoder(
    val maxFrameSize: Int,
) : ByteToMessageDecoder() {
    init {
        require(maxFrameSize >= 0) { "maxFrameSize must be non-negative" }
    }

    override fun decode(
        context: io.netty.channel.ChannelHandlerContext,
        input: ByteBuf,
        output: MutableList<Any>,
    ) {
        if (input.readableBytes() < FRAME_HEADER_SIZE) return
        val declared = input.getUnsignedInt(input.readerIndex())
        if (declared > maxFrameSize.toLong()) {
            throw TrevRpcException(
                Status.resourceExhausted(
                    "declared frame body is $declared bytes, maximum is $maxFrameSize",
                ),
            )
        }
        val frameSize = declared.toInt()
        if (input.readableBytes() < FRAME_HEADER_SIZE + frameSize) return
        input.skipBytes(FRAME_HEADER_SIZE)
        output += input.readRetainedSlice(frameSize)
    }

    override fun decodeLast(
        context: io.netty.channel.ChannelHandlerContext,
        input: ByteBuf,
        output: MutableList<Any>,
    ) {
        decode(context, input, output)
        if (input.isReadable) {
            throw TrevRpcException(Status.invalidArgument("framed stream ended with a partial frame"))
        }
    }
}

object TrevRpcFrameWriter {
    /** Returns a new buffer owned by the caller. */
    fun encode(
        allocator: ByteBufAllocator,
        body: ByteArray,
        maxFrameSize: Int,
    ): ByteBuf {
        require(maxFrameSize >= 0) { "maxFrameSize must be non-negative" }
        if (body.size > maxFrameSize) {
            throw TrevRpcException(
                Status.resourceExhausted("frame body is ${body.size} bytes, maximum is $maxFrameSize"),
            )
        }
        return allocator.buffer(FRAME_HEADER_SIZE + body.size, FRAME_HEADER_SIZE + body.size).apply {
            writeInt(body.size)
            writeBytes(body)
        }
    }

    /** Transfers ownership of the allocated frame to [channel]. */
    fun write(
        channel: Channel,
        body: ByteArray,
        maxFrameSize: Int,
    ): ChannelFuture {
        val frame = encode(channel.alloc(), body, maxFrameSize)
        return try {
            channel.writeAndFlush(frame)
        } catch (error: Throwable) {
            frame.release()
            throw error
        }
    }
}

internal class IncrementalFrameReader(
    private val maxFrameSize: Int,
) {
    private val header = ByteArray(FRAME_HEADER_SIZE)
    private var headerBytes = 0
    private var body: ByteArray? = null
    private var bodyBytes = 0

    fun feed(input: ByteBuf): List<ByteArray> {
        val output = mutableListOf<ByteArray>()
        while (input.isReadable || body?.isEmpty() == true) {
            if (body == null) {
                val count = minOf(FRAME_HEADER_SIZE - headerBytes, input.readableBytes())
                input.readBytes(header, headerBytes, count)
                headerBytes += count
                if (headerBytes != FRAME_HEADER_SIZE) break
                val declared =
                    ((header[0].toLong() and 0xff) shl 24) or
                        ((header[1].toLong() and 0xff) shl 16) or
                        ((header[2].toLong() and 0xff) shl 8) or
                        (header[3].toLong() and 0xff)
                if (declared > maxFrameSize) {
                    throw TrevRpcException(
                        Status.resourceExhausted(
                            "declared frame body is $declared bytes, maximum is $maxFrameSize",
                        ),
                    )
                }
                body = ByteArray(declared.toInt())
            }
            val current = checkNotNull(body)
            val count = minOf(current.size - bodyBytes, input.readableBytes())
            input.readBytes(current, bodyBytes, count)
            bodyBytes += count
            if (bodyBytes != current.size) break
            output += current
            body = null
            bodyBytes = 0
            headerBytes = 0
        }
        return output
    }

    fun finish() {
        if (headerBytes != 0 || body != null) {
            throw TrevRpcException(Status.invalidArgument("framed stream ended with a partial frame"))
        }
    }
}
