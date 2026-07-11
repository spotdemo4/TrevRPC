package zip.trev.trevrpc.netty

import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import zip.trev.trevrpc.TrevRpcException
import zip.trev.trevrpc.netty.advanced.Http3FrameInbox

class Http3ClientValidationTest {
    @Test
    fun `accepts exactly status 200 and TrevRPC content type`() {
        runBlocking {
            val inbox = Http3FrameInbox(NettyTransportOptions())
            val channel = EmbeddedChannel(inbox)
            val headers = DefaultHttp3HeadersFrame()
            headers.headers().status("200").set("content-type", "Application/TrevRPC")

            channel.writeInbound(headers)

            inbox.awaitSuccess()
            channel.finishAndReleaseAll()
        }
    }

    @Test
    fun `rejects non-200 missing parameterized combined and duplicate content types`() {
        listOf(
            response("204", listOf(TREV_RPC_MEDIA_TYPE)),
            response("200", emptyList()),
            response("200", listOf("application/trevrpc; charset=utf-8")),
            response("200", listOf("application/trevrpc, application/trevrpc")),
            response("200", listOf(TREV_RPC_MEDIA_TYPE, TREV_RPC_MEDIA_TYPE)),
        ).forEach { headers ->
            val inbox = Http3FrameInbox(NettyTransportOptions())
            val channel = EmbeddedChannel(inbox)
            channel.writeInbound(headers)

            assertThrows(TrevRpcException::class.java) { runBlocking { inbox.awaitSuccess() } }
            channel.finishAndReleaseAll()
        }
    }

    @Test
    fun `rejects duplicate headers and trailers after valid response`() {
        runBlocking {
            val inbox = Http3FrameInbox(NettyTransportOptions())
            val channel = EmbeddedChannel(inbox)
            channel.writeInbound(response("200", listOf(TREV_RPC_MEDIA_TYPE)))
            inbox.awaitSuccess()

            channel.writeInbound(DefaultHttp3HeadersFrame().apply { headers().set("x-trailer", "no") })

            assertThrows(TrevRpcException::class.java) { runBlocking { inbox.receive() } }
            channel.finishAndReleaseAll()
        }
    }

    private fun response(
        status: String,
        contentTypes: List<String>,
    ): DefaultHttp3HeadersFrame =
        DefaultHttp3HeadersFrame().apply {
            headers().status(status)
            contentTypes.forEach { headers().add("content-type", it) }
        }
}
