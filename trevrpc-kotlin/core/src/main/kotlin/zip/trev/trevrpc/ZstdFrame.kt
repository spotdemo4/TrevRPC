package zip.trev.trevrpc

import java.security.MessageDigest

/** What a receiver needs from a ZSTD_MESSAGE payload before decompressing it. */
internal class ZstdHeader(
    /** The Dictionary_ID; never zero. */
    val dictionaryId: UInt,
    /** The Frame_Content_Size: the size of the message, saturated at [Long.MAX_VALUE]. */
    val contentSize: Long,
)

/** Opens every zstd-format dictionary (RFC 8878, section 5). */
private const val DICTIONARY_MAGIC = 0xEC30A437.toInt()

/**
 * Validates that [payload] is exactly one magicless zstd frame with a non-zero Dictionary_ID and a
 * Frame_Content_Size, and returns both, or null if it is not. The block headers are walked to find
 * the frame's end, so trailing bytes, including a second frame, are rejected. Block contents are
 * not checked; decompression does that.
 */
internal fun parseZstdHeader(payload: ByteArray): ZstdHeader? {
    if (payload.isEmpty()) return null
    val descriptor = payload[0].toInt() and 0xff
    val contentSizeFlag = descriptor shr 6
    val singleSegment = descriptor and 0x20 != 0
    if (descriptor and 0x08 != 0) return null // reserved bit
    val checksum = descriptor and 0x04 != 0
    val dictionaryIdSize = intArrayOf(0, 1, 2, 4)[descriptor and 0x03]
    val contentSizeSize = if (contentSizeFlag == 0 && singleSegment) 1 else intArrayOf(0, 2, 4, 8)[contentSizeFlag]
    if (dictionaryIdSize == 0 || contentSizeSize == 0) return null
    var position = if (singleSegment) 1 else 2 // the Window_Descriptor
    if (payload.size < position + dictionaryIdSize + contentSizeSize) return null
    val dictionaryId = littleEndian(payload, position, dictionaryIdSize).toUInt()
    position += dictionaryIdSize
    var contentSize = littleEndian(payload, position, contentSizeSize)
    if (contentSizeSize == 2) contentSize += 256
    position += contentSizeSize
    if (dictionaryId == 0u) return null
    while (true) {
        val block = nextBlock(payload, position) ?: return null
        position = block.next
        if (block.last) break
    }
    if (checksum) position += 4
    if (position != payload.size) return null
    // An 8-byte size above Long.MAX_VALUE reads as negative; it exceeds every limit either way.
    return ZstdHeader(dictionaryId, if (contentSize < 0) Long.MAX_VALUE else contentSize)
}

/** One zstd block: where the next one starts, and whether this one is the Last_Block. */
private class Block(
    val next: Int,
    val last: Boolean,
)

/**
 * Walks the block whose header starts at [position], or returns null for a reserved block type or
 * a block that does not fit in [data]. Block contents are not checked.
 */
private fun nextBlock(
    data: ByteArray,
    position: Int,
): Block? {
    if (data.size - position < 3) return null
    val header = littleEndian(data, position, 3).toInt()
    // Raw_Block and Compressed_Block carry their size; an RLE_Block repeats one byte.
    val size =
        when ((header shr 1) and 0x03) {
            0, 2 -> header ushr 3
            1 -> 1
            else -> return null
        }
    if (data.size - position - 3 < size) return null
    return Block(position + 3 + size, header and 1 != 0)
}

/** The window TrevRPC's stream compression uses and decodes, as a base-2 logarithm. */
internal const val STREAM_WINDOW_LOG = 16

/** The smallest window a peer may advertise; a smaller `max_window_log` enables nothing. */
internal const val MIN_STREAM_WINDOW_LOG = 10

/**
 * What a receiver learns from validating the zstd data of a ZSTD_STREAM_MESSAGE: whether it starts
 * a frame and, if so, that frame's Dictionary_ID (zero for none), and whether it ends the frame.
 */
internal class ZstdStreamChunk(
    val startsFrame: Boolean,
    val dictionaryId: UInt,
    val endsFrame: Boolean,
)

/**
 * Validates [chunk], the zstd data of a ZSTD_STREAM_MESSAGE, or returns null if it is invalid.
 * [atFrameStart] says that no frame is open, so a non-empty chunk must start with a frame header
 * without Single_Segment_flag, Frame_Content_Size_flag, Content_Checksum_flag or the reserved bit,
 * whose window is at most 2^[maxWindowLog]. The block headers are then walked: a chunk is whole
 * blocks, and a Last_Block ends it. An empty chunk is an empty message and changes nothing.
 */
internal fun parseZstdStreamChunk(
    chunk: ByteArray,
    atFrameStart: Boolean,
    maxWindowLog: Int = STREAM_WINDOW_LOG,
): ZstdStreamChunk? {
    if (chunk.isEmpty()) return ZstdStreamChunk(startsFrame = false, dictionaryId = 0u, endsFrame = false)
    var position = 0
    var dictionaryId = 0u
    if (atFrameStart) {
        val descriptor = chunk[0].toInt() and 0xff
        // Frame_Content_Size_flag (bits 7-6), Single_Segment_flag (bit 5), the reserved bit (bit 3)
        // and Content_Checksum_flag (bit 2) must be unset.
        if (descriptor and 0xEC != 0 || chunk.size < 2) return null
        val windowDescriptor = chunk[1].toInt() and 0xff
        val base = 1L shl (10 + (windowDescriptor ushr 3))
        if (base + base / 8 * (windowDescriptor and 7) > 1L shl maxWindowLog) return null
        val dictionaryIdSize = intArrayOf(0, 1, 2, 4)[descriptor and 0x03]
        position = 2 + dictionaryIdSize
        if (chunk.size < position) return null
        dictionaryId = littleEndian(chunk, 2, dictionaryIdSize).toUInt()
    }
    var endsFrame = false
    while (position < chunk.size) {
        val block = nextBlock(chunk, position) ?: return null
        if (block.last && block.next != chunk.size) return null
        position = block.next
        endsFrame = block.last
    }
    return ZstdStreamChunk(atFrameStart, dictionaryId, endsFrame)
}

/**
 * The Dictionary_ID of a zstd-format dictionary. Raw-content dictionaries have no ID, and zero is
 * not one, so both are rejected.
 */
internal fun zstdDictionaryId(dictionary: ByteArray): UInt {
    require(dictionary.size >= 8 && littleEndian(dictionary, 0, 4).toInt() == DICTIONARY_MAGIC) {
        "not a zstd-format dictionary"
    }
    val id = littleEndian(dictionary, 4, 4).toUInt()
    require(id != 0u) { "dictionary ID is zero" }
    return id
}

/** A dictionary's fingerprint: the first 8 bytes of its SHA-256 digest. */
internal fun zstdFingerprint(dictionary: ByteArray): ByteArray = MessageDigest.getInstance("SHA-256").digest(dictionary).copyOf(8)

private fun littleEndian(
    bytes: ByteArray,
    offset: Int,
    size: Int,
): Long {
    var value = 0L
    for (index in 0 until size) value = value or ((bytes[offset + index].toLong() and 0xff) shl (8 * index))
    return value
}
