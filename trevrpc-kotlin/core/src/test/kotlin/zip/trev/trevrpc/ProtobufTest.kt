package zip.trev.trevrpc

import com.google.protobuf.CodedOutputStream
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.StringValue
import com.google.protobuf.WireFormat
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream

class ProtobufTest {
    // A message with `string name = 1`, `repeated int32 ids = 2` (packable), `fixed64 at = 3`, and
    // `int32 big = 536870911`, the highest field number, whose tags are negative Ints.
    private val wireTypes =
        ProtobufWireTypes(
            tag(536_870_911, WireFormat.WIRETYPE_VARINT),
            tag(3, WireFormat.WIRETYPE_FIXED64),
            tag(2, WireFormat.WIRETYPE_LENGTH_DELIMITED),
            tag(2, WireFormat.WIRETYPE_VARINT),
            tag(1, WireFormat.WIRETYPE_LENGTH_DELIMITED),
        )

    private fun tag(
        field: Int,
        wireType: Int,
    ): Int = (field shl 3) or wireType

    private fun encode(write: CodedOutputStream.() -> Unit): ByteArray {
        val bytes = ByteArrayOutputStream()
        val output = CodedOutputStream.newInstance(bytes)
        output.write()
        output.flush()
        return bytes.toByteArray()
    }

    private fun decode(body: ByteArray): ByteArray = decodeProtobuf(body, wireTypes) { it }

    // The test task runs on protobuf-javalite and testProtobufJava on protobuf-java, which alone
    // has descriptors; a classpath that loses its runtime fails here instead of testing the other.
    @Test
    fun `tests run on the protobuf runtime their task names`() {
        val full = runCatching { Class.forName("com.google.protobuf.Descriptors") }.isSuccess
        assertEquals(System.getProperty("trevrpc.protobuf.runtime"), if (full) "java" else "javalite")
    }

    @Test
    fun `fields with their declared wire types and unknown fields decode`() {
        val body =
            encode {
                writeString(1, "name")
                writeInt32(2, 7)
                writeFixed64(3, 9)
                writeInt32(536_870_911, 1)
                // Unknown fields may use any wire type, groups included.
                writeFixed32(4, 1)
                writeTag(5, WireFormat.WIRETYPE_START_GROUP)
                writeInt32(1, 2)
                writeTag(5, WireFormat.WIRETYPE_END_GROUP)
            }
        assertArrayEquals(body, decode(body))
    }

    @Test
    fun `a repeated scalar field decodes packed and unpacked`() {
        val packed =
            encode {
                writeTag(2, WireFormat.WIRETYPE_LENGTH_DELIMITED)
                writeUInt32NoTag(2)
                writeInt32NoTag(1)
                writeInt32NoTag(2)
            }
        assertArrayEquals(packed, decode(packed))
        val unpacked = encode { writeInt32(2, 1) }
        assertArrayEquals(unpacked, decode(unpacked))
    }

    @Test
    fun `a known field with another wire type fails`() {
        for (body in listOf(
            encode { writeInt32(1, 1) },
            encode { writeFixed32(2, 1) },
            encode { writeString(3, "x") },
            encode { writeFixed64(536_870_911, 1) },
        )) {
            val error = assertThrows(InvalidProtocolBufferException::class.java) { decode(body) }
            assertTrue(error.message!!.contains("wire type"), error.message)
        }
    }

    @Test
    fun `a stray end group tag fails`() {
        assertThrows(InvalidProtocolBufferException::class.java) { decode(encode { writeTag(6, WireFormat.WIRETYPE_END_GROUP) }) }
    }

    @Test
    fun `a codec encodes and decodes lite messages`() {
        val codec =
            protobufCodec(
                "google.protobuf.StringValue",
                ProtobufWireTypes(tag(1, WireFormat.WIRETYPE_LENGTH_DELIMITED)),
                StringValue::parseFrom,
            )
        assertEquals("google.protobuf.StringValue", codec.messageType)
        assertEquals("hi", codec.decode(codec.encode(StringValue.of("hi"))).value)
        assertThrows(InvalidProtocolBufferException::class.java) { codec.decode(encode { writeInt32(1, 1) }) }
    }
}
