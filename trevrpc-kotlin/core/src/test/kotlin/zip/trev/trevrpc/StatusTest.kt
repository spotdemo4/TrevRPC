package zip.trev.trevrpc

import com.google.protobuf.Int32Value
import com.google.protobuf.StringValue
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds

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

    private fun hex(text: String): ByteArray = text.chunked(2).map { it.toInt(16).toByte() }.toByteArray()

    private fun retryInfo(text: String) = StatusDetail("google.rpc.RetryInfo", hex(text))

    private fun retryDelay(vararg details: StatusDetail): Duration? =
        Status.resourceExhausted("slow down").withDetails(details.toList()).retryDelay

    @Test
    fun `the retry delay is read from a RetryInfo detail`() {
        // RetryInfo{retry_delay: Duration{seconds: 1, nanos: 2}} is the same bytes in every runtime's tests.
        assertEquals(1.seconds + 2.nanoseconds, retryDelay(retryInfo("0a0408011002")))
        assertEquals(30.seconds, retryDelay(retryInfo("0a02081e")))
        assertEquals(500.milliseconds, retryDelay(retryInfo("0a061080cab5ee01")))
        assertEquals(1500.milliseconds, retryDelay(retryInfo("0a0808011080cab5ee01")))
        assertEquals(Duration.ZERO, retryDelay(retryInfo("0a00")))
        assertNull(Status.resourceExhausted("no hint").retryDelay)
        val exception = TrevRpcException(Status.resourceExhausted("slow").withDetails(listOf(retryInfo("0a0808011080cab5ee01"))))
        assertEquals(1500.milliseconds, exception.retryDelay)
    }

    @Test
    fun `the retry delay follows the protobuf rules of RetryInfo`() {
        // Unknown fields are skipped, at either level, and the last value wins.
        assertEquals(1.seconds + 2.nanoseconds, retryDelay(retryInfo("1a036162630a04080110022000")))
        assertEquals(1.seconds + 2.nanoseconds, retryDelay(retryInfo("0a061a0008011002")))
        assertEquals(11.seconds, retryDelay(retryInfo("0a02080a0a02080b")))
        // No delay, a malformed one, or an invalid one.
        val invalid =
            listOf(
                "",
                "1a03616263",
                "0a",
                "0a040801",
                "0a0108",
                "0801",
                "0a050d00000000",
                "0a0b08ffffffffffffffffff01",
                "0a0d080110ffffffffffffffffff01",
                "0a080801108094ebdc03",
            )
        for (text in invalid) {
            assertNull(retryDelay(retryInfo(text)), text)
        }
    }

    @Test
    fun `the retry delay skips details that are not a well-formed RetryInfo`() {
        val delay = retryDelay(StatusDetail("other.Type", hex("0a020809")), retryInfo("0a09"), retryInfo("0a020803"))

        assertEquals(3.seconds, delay)
    }
}
