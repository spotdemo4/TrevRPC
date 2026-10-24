package zip.trev.trevrpc

import com.github.luben.zstd.Zstd
import com.github.luben.zstd.ZstdCompressCtx
import com.github.luben.zstd.ZstdDecompressCtx
import com.github.luben.zstd.ZstdDictCompress
import com.github.luben.zstd.ZstdDictDecompress
import com.github.luben.zstd.ZstdException
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * A dictionary digested by zstd-jni for both directions. This is the only class that refers to
 * zstd-jni, which the application supplies, so a client that configures no [Compression] never
 * loads it. It is safe for concurrent use.
 */
internal class ZstdCodec(
    dictionary: ByteArray,
    private val level: Int,
) {
    private val compressDictionary = ZstdDictCompress(dictionary, level)
    private val decompressDictionary = ZstdDictDecompress(dictionary)

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
