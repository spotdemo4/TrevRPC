package zip.trev.trevrpc

/**
 * QUIC variable-length integers (RFC 9000, section 16). Values are `0..2^62-1` and are carried as
 * [Long]. The two most significant bits of the first byte give the encoded length (1, 2, 4 or 8).
 */
object QuicVarint {
    const val MAX_VALUE: Long = (1L shl 62) - 1

    /** The minimal encoded size of [value] in bytes. */
    fun size(value: Long): Int {
        require(value in 0..MAX_VALUE) { "QUIC varint out of range: $value" }
        return when {
            value < (1L shl 6) -> 1
            value < (1L shl 14) -> 2
            value < (1L shl 30) -> 4
            else -> 8
        }
    }

    /** The encoded size announced by [firstByte], which may be a non-minimal encoding. */
    fun encodedLength(firstByte: Byte): Int = 1 shl ((firstByte.toInt() and 0xff) ushr 6)

    /** Writes the minimal encoding of [value] at [offset] and returns the offset after it. */
    fun write(
        output: ByteArray,
        offset: Int,
        value: Long,
    ): Int {
        val size = size(value)
        val prefix = (Integer.numberOfTrailingZeros(size)).toLong() shl 6
        for (index in 0 until size) {
            val shift = 8 * (size - 1 - index)
            var byte = (value ushr shift) and 0xff
            if (index == 0) byte = byte or prefix
            output[offset + index] = byte.toByte()
        }
        return offset + size
    }

    fun encode(value: Long): ByteArray = ByteArray(size(value)).also { write(it, 0, value) }

    /**
     * Decodes the integer starting at [offset], accepting any valid (including non-minimal)
     * encoding. Throws [IllegalArgumentException] when [bytes] ends inside the encoding.
     */
    fun decode(
        bytes: ByteArray,
        offset: Int = 0,
    ): Long {
        require(offset in bytes.indices) { "QUIC varint is truncated" }
        val length = encodedLength(bytes[offset])
        require(bytes.size - offset >= length) { "QUIC varint is truncated" }
        var value = (bytes[offset].toInt() and 0x3f).toLong()
        for (index in 1 until length) {
            value = (value shl 8) or (bytes[offset + index].toLong() and 0xff)
        }
        return value
    }
}
