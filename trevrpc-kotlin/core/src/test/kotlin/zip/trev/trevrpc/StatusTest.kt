package zip.trev.trevrpc

import com.google.protobuf.Int32Value
import com.google.protobuf.StringValue
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class StatusTest {
    @Test
    fun `details decode only those of the requested type`() {
        val status =
            Status.invalidArgument("bad").withDetails(
                listOf(StatusDetail("other.Type", byteArrayOf(1)), StatusDetail.of(StringValue.of("why"))),
            )

        assertEquals(listOf("why"), status.findDetails(StringValue.getDefaultInstance()).map { it.value })
        assertEquals(emptyList<Int32Value>(), status.findDetails(Int32Value.getDefaultInstance()))
        assertEquals(status, WireCodec.decodeStatus(WireCodec.encode(RpcStatus.fromStatus(status))).status)
    }
}
