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
        if (payload.size - position < 3) return null
        val blockHeader = littleEndian(payload, position, 3).toInt()
        position += 3
        // Raw_Block and Compressed_Block carry their size; an RLE_Block repeats one byte.
        val size =
            when ((blockHeader shr 1) and 0x03) {
                0, 2 -> blockHeader ushr 3
                1 -> 1
                else -> return null
            }
        if (payload.size - position < size) return null
        position += size
        if (blockHeader and 1 != 0) break // Last_Block
    }
    if (checksum) position += 4
    if (position != payload.size) return null
    // An 8-byte size above Long.MAX_VALUE reads as negative; it exceeds every limit either way.
    return ZstdHeader(dictionaryId, if (contentSize < 0) Long.MAX_VALUE else contentSize)
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
