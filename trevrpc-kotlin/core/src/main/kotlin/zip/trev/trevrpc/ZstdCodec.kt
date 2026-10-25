package zip.trev.trevrpc

import com.github.luben.zstd.EndDirective
import com.github.luben.zstd.Zstd
import com.github.luben.zstd.ZstdCompressCtx
import com.github.luben.zstd.ZstdDecompressCtx
import com.github.luben.zstd.ZstdDictCompress
import com.github.luben.zstd.ZstdDictDecompress
import com.github.luben.zstd.ZstdException
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue

// This file is the only one that refers to zstd-jni, which the application supplies, so a client
// that configures no [Compression] never loads it.

/** Loads zstd-jni, or throws the [LinkageError] that says it is missing. */
internal fun loadZstd() {
    Zstd.compressBound(0)
}

/** A dictionary digested by zstd-jni for both directions. It is safe for concurrent use. */
internal class ZstdCodec(
    dictionary: ByteArray,
    private val level: Int,
) {
    internal val compressDictionary = ZstdDictCompress(dictionary, level)
    internal val decompressDictionary = ZstdDictDecompress(dictionary)

    // Contexts are pooled: creating one per message costs about as much as compressing a small
    // message. The pools hold at most one context per concurrent use.
    private val compressors = ConcurrentLinkedQueue<ZstdCompressCtx>()
    private val decompressors = ConcurrentLinkedQueue<ZstdDecompressCtx>()

    /** Compresses [message] into a ZSTD_MESSAGE payload, or returns null if that is not shorter. */
    fun compress(message: ByteArray): ByteArray? {
        val context = compressors.poll() ?: newCompressor()
        try {
            val output = ByteArray(Zstd.compressBound(message.size.toLong()).toInt())
            val size = context.compressByteArray(output, 0, output.size, message, 0, message.size)
            return if (size < message.size) output.copyOf(size) else null
        } finally {
            compressors.offer(context)
        }
    }

    /**
     * Decompresses a payload whose [header] came from [parseZstdHeader], or returns null if it
     * fails to decompress to exactly the declared size. The caller has checked the size against
     * its limits.
     */
    fun decompress(
        payload: ByteArray,
        header: ZstdHeader,
    ): ByteArray? {
        val message = ByteArray(header.contentSize.toInt())
        val context = decompressors.poll() ?: newDecompressor()
        try {
            val size = context.decompressByteArray(message, 0, message.size, payload, 0, payload.size)
            return if (size == message.size) message else null
        } catch (_: ZstdException) {
            return null
        } finally {
            decompressors.offer(context)
        }
    }

    private fun newCompressor(): ZstdCompressCtx =
        ZstdCompressCtx()
            .setLevel(level)
            .setMagicless(true)
            .setChecksum(false)
            .setDictID(true)
            .setContentSize(true)
            .loadDict(compressDictionary)

    private fun newDecompressor(): ZstdDecompressCtx = ZstdDecompressCtx().setMagicless(true).loadDict(decompressDictionary)
}

/**
 * The compressing context of one direction of a call in stream compression: every message is
 * compressed against the earlier ones and flushed as one chunk. It starts from [dictionary] when
 * there is one. One sender uses it at a time; [close] frees it.
 */
internal class ZstdStreamEncoder(
    level: Int,
    windowLog: Int,
    dictionary: ZstdCodec?,
) {
    private val context =
        ZstdCompressCtx()
            .setLevel(level)
            .setMagicless(true)
            .setChecksum(false)
            .setDictID(true)
            .setContentSize(false)
            .setWindowLog(windowLog)
            .also { context -> dictionary?.let { context.loadDict(it.compressDictionary) } }

    /** The bytes of the messages in the current frame. */
    private var inFrame = 0

    /**
     * Compresses [message] into its chunk, ending the frame once it holds [FRAME_LIMIT] bytes. An
     * empty message is an empty chunk, and never ends a frame.
     */
    fun compress(message: ByteArray): ByteArray {
        val end = inFrame + message.size >= FRAME_LIMIT
        inFrame = if (end) 0 else inFrame + message.size
        val directive = if (end) EndDirective.END else EndDirective.FLUSH
        val input = ByteBuffer.wrap(message)
        // The bound leaves room for the frame header of the first chunk too.
        var output = ByteBuffer.allocate(Zstd.compressBound(message.size.toLong()).toInt() + FRAME_HEADER_ROOM)
        while (!context.compressByteBufferStream(output, input, directive)) {
            if (!output.hasRemaining()) {
                output = ByteBuffer.allocate(output.capacity() * 2).put(output.array(), 0, output.position())
            }
        }
        return output.array().copyOf(output.position())
    }

    fun close() = context.close()

    internal companion object {
        const val FRAME_HEADER_ROOM = 32

        /**
         * How many bytes of messages a frame takes before the encoder ends it and starts the next.
         * libzstd's streaming buffer wraps once a 64 KiB window has filled, and from then on
         * matches its history in two pieces, which costs a quarter more CPU per message. Ending the
         * frame first keeps the history in one piece and makes messages less than 1% larger, since
         * the next frame starts afresh (or from the dictionary again).
         */
        const val FRAME_LIMIT = 60 shl 10
    }
}

/**
 * The decompressing context of one direction of a call in stream compression. Each frame gets a
 * fresh libzstd context holding only that frame's dictionary, so a frame never inherits the
 * previous one's. One receiver uses it at a time; [close] frees it.
 */
internal class ZstdStreamDecoder {
    private var context: ZstdDecompressCtx? = null

    /** Starts a new frame, which uses [dictionary], or none. */
    fun startFrame(dictionary: ZstdCodec?) {
        context?.close()
        context = ZstdDecompressCtx().setMagicless(true).also { context -> dictionary?.let { context.loadDict(it.decompressDictionary) } }
    }

    /**
     * Decompresses a validated [chunk], or returns null unless it decompresses to exactly [size]
     * bytes. libzstd holds back output that does not fit, so the output has room for one byte more,
     * which a message longer than declared then fills.
     */
    fun decompress(
        chunk: ByteArray,
        size: Int,
    ): ByteArray? {
        if (chunk.isEmpty()) return if (size == 0) ByteArray(0) else null
        val context = context ?: return null
        val input = ByteBuffer.wrap(chunk)
        val output = ByteBuffer.allocate(size + 1)
        try {
            while (input.hasRemaining()) {
                val consumed = input.position()
                val produced = output.position()
                context.decompressByteBufferStream(output, input)
                if (input.position() == consumed && output.position() == produced) return null
            }
        } catch (_: RuntimeException) {
            return null
        }
        return if (output.position() == size) output.array().copyOf(size) else null
    }

    fun close() {
        context?.close()
        context = null
    }
}
