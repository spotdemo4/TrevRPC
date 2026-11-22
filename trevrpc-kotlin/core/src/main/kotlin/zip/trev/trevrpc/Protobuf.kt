package zip.trev.trevrpc

import com.google.protobuf.CodedInputStream
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.MessageLite

/**
 * The tags a protobuf message's fields may arrive with: each field's number and the wire type of its
 * declared type, plus the length-delimited wire type for a repeated scalar field, which may be packed.
 * The generator computes them from the schema, so decoding needs no descriptor and works the same
 * with protobuf-java and protobuf-javalite.
 *
 * A tag is `(fieldNumber shl 3) or wireType`, as protobuf encodes it; field numbers from 2^28 up
 * wrap to negative tags.
 */
public class ProtobufWireTypes(
    vararg tags: Int,
) {
    // Sorted, so the tags of one field are adjacent: they differ only in their low three bits.
    private val tags: IntArray = tags.copyOf().also(IntArray::sort)

    /** Throws when [tag] is that of a known field but carries a wire type the field does not use. */
    internal fun check(tag: Int) {
        val index = tags.binarySearch(tag)
        if (index >= 0) return
        val insertion = -index - 1
        val field = tag ushr 3
        val known =
            (insertion < tags.size && tags[insertion] ushr 3 == field) ||
                (insertion > 0 && tags[insertion - 1] ushr 3 == field)
        if (known) {
            throw InvalidProtocolBufferException("field $field used wire type ${tag and 7}, which its type does not use")
        }
    }
}

/**
 * Decodes [body] with [parser] after it checks that no known field arrives with a wire type its
 * declared type does not use. Such a field fails the decode, where protobuf-java would keep it as an
 * unknown field and leave the declared field unset.
 */
public fun <T> decodeProtobuf(
    body: ByteArray,
    wireTypes: ProtobufWireTypes,
    parser: (ByteArray) -> T,
): T {
    val input = CodedInputStream.newInstance(body)
    while (true) {
        val tag = input.readTag()
        if (tag == 0) break
        wireTypes.check(tag)
        if (!input.skipField(tag)) {
            throw InvalidProtocolBufferException("unexpected end-group tag")
        }
    }
    return parser(body)
}

/**
 * The codec of a protobuf message type, which generated clients build for each message type their
 * methods use. [messageType] is the type's full protobuf name, which picks its compression dictionary.
 */
public fun <T : MessageLite> protobufCodec(
    messageType: String,
    wireTypes: ProtobufWireTypes,
    parse: (ByteArray) -> T,
): MessageCodec<T> =
    MessageCodec(
        encode = MessageLite::toByteArray,
        decode = { body -> decodeProtobuf(body, wireTypes, parse) },
        messageType = messageType,
    )
