package zip.trev.trevrpc.netty

import io.netty.channel.embedded.EmbeddedChannel
import io.netty.channel.socket.ChannelInputShutdownEvent
import io.netty.handler.codec.http3.DefaultHttp3DataFrame
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import zip.trev.trevrpc.Code
import zip.trev.trevrpc.RpcKind
import zip.trev.trevrpc.RpcStreamFrame
import zip.trev.trevrpc.Status
import zip.trev.trevrpc.TrevRpcException
import zip.trev.trevrpc.WireCodec
import zip.trev.trevrpc.netty.advanced.Http3FrameInbox
import zip.trev.trevrpc.netty.advanced.TerminalStreamEnd
import zip.trev.trevrpc.netty.advanced.finishHttp3StreamingRequest
import zip.trev.trevrpc.netty.advanced.http3RequestEndsAfterInitialData
import zip.trev.trevrpc.netty.advanced.performHttp3RequestHandshake
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

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

    @Test
    fun `partial response data after terminal status is trailing data rather than missing FIN`(): Unit =
        runBlocking {
            val inbox = Http3FrameInbox(NettyTransportOptions())
            val channel = EmbeddedChannel(inbox)
            channel.writeInbound(response("200", listOf(TREV_RPC_MEDIA_TYPE)))
            val terminal = WireCodec.encode(RpcStreamFrame.status(Status.notFound("remote status")))
            channel.writeInbound(
                DefaultHttp3DataFrame(
                    TrevRpcFrameWriter.encode(channel.alloc(), terminal, zip.trev.trevrpc.DEFAULT_MAX_FRAME_SIZE),
                ),
            )
            channel.writeInbound(DefaultHttp3DataFrame(channel.alloc().buffer(1).writeByte(0)))

            inbox.awaitSuccess()
            assertEquals(Status.notFound("remote status"), WireCodec.decodeStreamFrame(checkNotNull(inbox.receive())).status)
            channel.pipeline().fireUserEventTriggered(ChannelInputShutdownEvent.INSTANCE)

            assertTrue(inbox.awaitEnd(1.seconds) is TerminalStreamEnd.TrailingData)
            channel.finishAndReleaseAll()
        }

    @Test
    fun `all RPC shapes finish request before awaiting end-dispatched response`() =
        runBlocking {
            RpcKind.entries.forEach { kind ->
                val requestFinished = CompletableDeferred<Unit>()
                val allowResponse = CompletableDeferred<Unit>()
                val events = mutableListOf<String>()
                val endsAfterInitialData = http3RequestEndsAfterInitialData(kind)
                val awaitResponse: suspend () -> Unit = {
                    assertEquals("fin", events.last())
                    allowResponse.await()
                    events += "response-headers"
                }

                val handshake =
                    async {
                        performHttp3RequestHandshake(
                            writeHeaders = { events += "headers" },
                            writeInitialData = { events += "initial-data" },
                            finishRequest = {
                                events += "fin"
                                requestFinished.complete(Unit)
                            },
                            awaitResponse = awaitResponse,
                            finishInitialRequest = endsAfterInitialData,
                        )
                    }

                if (endsAfterInitialData) {
                    requestFinished.await()
                    assertFalse(handshake.isCompleted)
                    allowResponse.complete(Unit)
                    handshake.await()
                } else {
                    handshake.await()
                    finishHttp3StreamingRequest(
                        writeEndFrame = { events += "end-data" },
                        finishRequest = {
                            events += "fin"
                            requestFinished.complete(Unit)
                        },
                    )
                    requestFinished.await()
                    assertEquals(listOf("headers", "initial-data", "end-data", "fin"), events)
                    allowResponse.complete(Unit)
                    awaitResponse()
                }

                assertEquals(
                    if (endsAfterInitialData) {
                        listOf("headers", "initial-data", "fin", "response-headers")
                    } else {
                        listOf("headers", "initial-data", "end-data", "fin", "response-headers")
                    },
                    events,
                    kind.name,
                )
            }
        }

    @Test
    fun `abortive close before response headers fails validation as unavailable`() =
        runBlocking {
            val inbox = Http3FrameInbox(NettyTransportOptions())
            val channel = EmbeddedChannel(inbox)

            channel.close().syncUninterruptibly()

            val error = assertThrows(TrevRpcException::class.java) { runBlocking { inbox.awaitSuccess() } }
            assertEquals(Code.UNAVAILABLE, error.status.code)
            channel.finishAndReleaseAll()
            Unit
        }

    @Test
    fun `abortive close after response headers fails receive as unavailable`() =
        runBlocking {
            val inbox = Http3FrameInbox(NettyTransportOptions())
            val channel = EmbeddedChannel(inbox)
            channel.writeInbound(response("200", listOf(TREV_RPC_MEDIA_TYPE)))
            inbox.awaitSuccess()

            channel.close().syncUninterruptibly()

            val error = assertThrows(TrevRpcException::class.java) { runBlocking { inbox.receive() } }
            assertEquals(Code.UNAVAILABLE, error.status.code)
            channel.finishAndReleaseAll()
            Unit
        }

    @Test
    fun `terminal response without FIN is bounded by transport idle timeout`() =
        runBlocking {
            val inbox = Http3FrameInbox(NettyTransportOptions(maxIdleTime = 10.milliseconds))
            val channel = EmbeddedChannel(inbox)
            channel.writeInbound(response("200", listOf(TREV_RPC_MEDIA_TYPE)))
            inbox.awaitSuccess()

            val error = assertThrows(TrevRpcException::class.java) { runBlocking { inbox.requireEnd() } }
            assertEquals(Code.UNAVAILABLE, error.status.code)
            channel.finishAndReleaseAll()
            Unit
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
