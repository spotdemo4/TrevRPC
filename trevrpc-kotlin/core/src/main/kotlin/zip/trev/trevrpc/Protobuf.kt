package zip.trev.trevrpc

import com.google.protobuf.CodedInputStream
import com.google.protobuf.Descriptors
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.WireFormat

public fun <T> decodeProtobuf(
    body: ByteArray,
    descriptor: Descriptors.Descriptor,
    parser: (ByteArray) -> T,
): T {
    validateKnownProtobufWireTypes(body, descriptor)
    return parser(body)
}

private fun validateKnownProtobufWireTypes(
    body: ByteArray,
    descriptor: Descriptors.Descriptor,
) {
    val input = CodedInputStream.newInstance(body)
    while (true) {
        val tag = input.readTag()
        if (tag == 0) return
        val field = descriptor.findFieldByNumber(WireFormat.getTagFieldNumber(tag))
        if (field != null) {
            val actualWireType = WireFormat.getTagWireType(tag)
            val packed =
                field.isRepeated &&
                    field.isPackable &&
                    actualWireType == WireFormat.WIRETYPE_LENGTH_DELIMITED
            if (!packed && actualWireType != field.liteType.wireType) {
                throw InvalidProtocolBufferException(
                    "field ${field.number} used wire type $actualWireType, expected ${field.liteType.wireType}",
                )
            }
        }
        if (!input.skipField(tag)) {
            throw InvalidProtocolBufferException("unexpected end-group tag")
        }
    }
}
