package zip.trev.trevrpc

import java.util.Collections
import java.util.Locale

class Metadata private constructor(
    initialValues: Map<String, ByteArray>,
) : Iterable<Map.Entry<String, ByteArray>> {
    private val values: Map<String, ByteArray> =
        Collections.unmodifiableMap(
            LinkedHashMap<String, ByteArray>(initialValues.size).apply {
                initialValues.forEach { (key, value) -> put(key, value.copyOf()) }
            },
        )

    val size: Int
        get() = values.size

    val isEmpty: Boolean
        get() = values.isEmpty()

    operator fun get(key: String): ByteArray? = values[key]?.copyOf()

    fun contains(key: String): Boolean = values.containsKey(key)

    fun toMap(): Map<String, ByteArray> =
        Collections.unmodifiableMap(
            LinkedHashMap<String, ByteArray>(values.size).apply {
                this@Metadata.values.entries.forEach { (key, value) -> put(key, value.copyOf()) }
            },
        )

    override fun iterator(): Iterator<Map.Entry<String, ByteArray>> = toMap().entries.iterator()

    override fun equals(other: Any?): Boolean =
        other is Metadata &&
            values.size == other.values.size &&
            values.all { (key, value) -> other.values[key]?.contentEquals(value) == true }

    override fun hashCode(): Int = values.entries.fold(0) { hash, (key, value) -> hash + (key.hashCode() xor value.contentHashCode()) }

    override fun toString(): String = "Metadata(keys=${values.keys})"

    internal fun wireEntries(): List<Map.Entry<String, ByteArray>> = values.entries.sortedBy { it.key }

    class Builder {
        private val values = linkedMapOf<String, ByteArray>()

        fun put(
            key: String,
            value: ByteArray,
        ): Builder =
            apply {
                values[normalizeKey(key)] = value.copyOf()
            }

        fun putAll(metadata: Metadata): Builder =
            apply {
                metadata.values.forEach { (key, value) -> values[key] = value.copyOf() }
            }

        fun build(): Metadata = create(values, normalize = false)
    }

    companion object {
        val EMPTY: Metadata = Metadata(emptyMap())

        fun builder(): Builder = Builder()

        fun of(vararg entries: Pair<String, ByteArray>): Metadata = create(linkedMapOf(*entries), normalize = true)

        fun from(entries: Map<String, ByteArray>): Metadata = create(entries, normalize = true)

        internal fun fromWire(entries: Map<String, ByteArray>): Metadata = create(entries, normalize = false)

        fun normalizeKey(key: String): String = key.lowercase(Locale.ROOT)

        private fun create(
            entries: Map<String, ByteArray>,
            normalize: Boolean,
        ): Metadata {
            val copied = linkedMapOf<String, ByteArray>()
            entries.forEach { (rawKey, value) ->
                val key = if (normalize) normalizeKey(rawKey) else rawKey
                copied[key] = value.copyOf()
            }
            validate(copied)
            return if (copied.isEmpty()) EMPTY else Metadata(copied)
        }

        private fun validate(entries: Map<String, ByteArray>) {
            invalidIf(entries.size > MAX_METADATA_ENTRIES) {
                "metadata has ${entries.size} entries, maximum is $MAX_METADATA_ENTRIES"
            }
            var total = 0L
            entries.forEach { (key, value) ->
                val keyBytes = key.toByteArray(Charsets.UTF_8)
                invalidIf(keyBytes.isEmpty()) { "metadata key is empty" }
                invalidIf(keyBytes.size > MAX_METADATA_KEY_LENGTH) {
                    "metadata key is ${keyBytes.size} bytes, maximum is $MAX_METADATA_KEY_LENGTH"
                }
                invalidIf(key.startsWith(RESERVED_METADATA_PREFIX)) {
                    "metadata key uses reserved prefix $RESERVED_METADATA_PREFIX"
                }
                invalidIf(key.any { it !in 'a'..'z' && it !in '0'..'9' && it !in "._-" }) {
                    "metadata key must use lowercase ASCII letters, digits, '.', '_' or '-'"
                }
                invalidIf(value.size > MAX_METADATA_VALUE_LENGTH) {
                    "metadata value '$key' is ${value.size} bytes, maximum is $MAX_METADATA_VALUE_LENGTH"
                }
                total += keyBytes.size.toLong() + value.size
            }
            invalidIf(total > MAX_METADATA_TOTAL_SIZE) {
                "metadata is $total bytes, maximum is $MAX_METADATA_TOTAL_SIZE"
            }
        }

        private inline fun invalidIf(
            condition: Boolean,
            message: () -> String,
        ) {
            if (condition) throw TrevRpcException(Status.invalidArgument("invalid metadata: ${message()}"))
        }
    }
}
