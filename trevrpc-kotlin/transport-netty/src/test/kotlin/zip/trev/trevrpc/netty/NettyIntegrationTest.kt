package zip.trev.trevrpc.netty

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame
import io.netty.handler.codec.http3.Http3
import io.netty.handler.codec.http3.Http3ClientConnectionHandler
import io.netty.handler.codec.http3.Http3DataFrame
import io.netty.handler.codec.http3.Http3HeadersFrame
import io.netty.handler.codec.http3.Http3RequestStreamInboundHandler
import io.netty.handler.codec.http3.TrevRpcWebTransportServerConnectionHandler
import io.netty.handler.codec.quic.QuicStreamType
import io.netty.handler.ssl.util.SelfSignedCertificate
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import zip.trev.trevrpc.BidirectionalStreamingHandler
import zip.trev.trevrpc.CallOptions
import zip.trev.trevrpc.Client
import zip.trev.trevrpc.ClientStreamingHandler
import zip.trev.trevrpc.Code
import zip.trev.trevrpc.MessageCodec
import zip.trev.trevrpc.ResponseEnvelope
import zip.trev.trevrpc.RpcRequest
import zip.trev.trevrpc.RpcResponse
import zip.trev.trevrpc.RpcTransport
import zip.trev.trevrpc.Server
import zip.trev.trevrpc.ServerOptions
import zip.trev.trevrpc.ServerStreamingHandler
import zip.trev.trevrpc.TrevRpcException
import zip.trev.trevrpc.UnaryHandler
import zip.trev.trevrpc.WireCodec
import zip.trev.trevrpc.netty.advanced.RawFrameInbox
import zip.trev.trevrpc.netty.advanced.RawNettyHttp3RpcTransport
import zip.trev.trevrpc.netty.advanced.RawNettyQuicRpcTransport
import zip.trev.trevrpc.netty.advanced.connectQuic
import java.net.InetAddress
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.seconds

class NettyIntegrationTest {
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    fun `one listener serves native HTTP3 and WebTransport while session remains active`() =
        runBlocking {
            val certificate = SelfSignedCertificate("localhost")
            val core = testServer()
            val transportServer =
                NettyRpcServer.bind(
                    core,
                    NettyRpcServerConfig(
                        bindAddress = InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                        tls =
                            NettyServerTls.Pem(
                                certificate.privateKey(),
                                certificate.certificate(),
                            ),
                        enableWebTransport = true,
                        webTransportAdmission = WebTransportAdmission { true },
                    ),
                )
            try {
                assertEquals(1, transportServer.bindCount)
                val config =
                    NettyQuicClientConfig(
                        remoteAddress = transportServer.localAddress,
                        tls = NettyClientTls("localhost", trustCertificates = listOf(certificate.cert())),
                    )
                val native = RawNettyQuicRpcTransport.connect(config)
                try {
                    exercise(native)
                } finally {
                    native.shutdown()
                }
                val h3 = RawNettyHttp3RpcTransport.connect(config)
                try {
                    exercise(h3)
                } finally {
                    h3.shutdown()
                }
                exerciseWebTransportAndConcurrentPost(config)
            } finally {
                transportServer.shutdown()
                certificate.delete()
            }
        }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    fun `native connection stream overload preserves status for every RPC shape`() =
        runBlocking {
            val certificate = SelfSignedCertificate("localhost")
            val handlerStarted = CompletableDeferred<Unit>()
            val core = testServer(ServerOptions(maxConcurrentStreamsPerConnection = 1))
            core.routeBidirectionalStreaming(
                "test.Service",
                "Bidi",
                BidirectionalStreamingHandler { _, requests ->
                    handlerStarted.complete(Unit)
                    ResponseEnvelope(requests.map { it + byteArrayOf(3) })
                },
            )
            val transportServer = bindNative(core, certificate)
            val transport = connectNative(transportServer, certificate)
            try {
                val client = Client(transport)
                val codec = MessageCodec.BYTE_ARRAY
                val held = client.bidirectionalStreaming("test.Service", "Bidi", codec, codec)
                handlerStarted.await()
                try {
                    assertCode(Code.UNAVAILABLE) {
                        client.unary("test.Service", "Unary", byteArrayOf(1), codec, codec)
                    }

                    val serverStream =
                        client.serverStreaming("test.Service", "ServerStream", byteArrayOf(1), codec, codec)
                    assertCode(Code.UNAVAILABLE) { serverStream.receive() }

                    val clientStream = client.clientStreaming("test.Service", "ClientStream", codec, codec)
                    assertCode(Code.UNAVAILABLE) { clientStream.receive() }

                    val bidi = client.bidirectionalStreaming("test.Service", "Bidi", codec, codec)
                    assertCode(Code.UNAVAILABLE) { bidi.receive() }
                } finally {
                    held.close()
                }
            } finally {
                transport.shutdown()
                transportServer.shutdown()
                certificate.delete()
            }
        }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    fun `native sequential unary calls release stream admission`() =
        runBlocking {
            val certificate = SelfSignedCertificate("localhost")
            val core = testServer(ServerOptions(maxConcurrentStreamsPerConnection = 1))
            val transportServer = bindNative(core, certificate)
            val transport = connectNative(transportServer, certificate)
            try {
                val client = Client(transport)
                val codec = MessageCodec.BYTE_ARRAY
                repeat(256) {
                    assertEquals(
                        listOf<Byte>(9, 1),
                        client.unary("test.Service", "Unary", byteArrayOf(9), codec, codec).toList(),
                    )
                }
            } finally {
                transport.shutdown()
                transportServer.shutdown()
                certificate.delete()
            }
        }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    fun `native concurrent server streams retire cleanly`(): Unit =
        runBlocking {
            val certificate = SelfSignedCertificate("localhost")
            val core = testServer(ServerOptions(maxConcurrentStreamsPerConnection = 4))
            core.routeServerStreaming(
                "test.Service",
                "ManyServerStream",
                ServerStreamingHandler { _, body ->
                    ResponseEnvelope(
                        flow {
                            repeat(128) { emit(body) }
                        },
                    )
                },
            )
            val transportOptions = NettyTransportOptions(workerParallelism = 4, maxIdleTime = 5.seconds)
            val transportServer = bindNative(core, certificate, transportOptions)
            val transport = connectNative(transportServer, certificate, transportOptions)
            try {
                val client = Client(transport)
                val codec = MessageCodec.BYTE_ARRAY
                val callOptions = CallOptions(maxResponseMessages = 128, streamIdleTimeout = 2.seconds)
                coroutineScope {
                    List(4) {
                        async {
                            repeat(1_000) {
                                val call =
                                    client.serverStreaming(
                                        "test.Service",
                                        "ManyServerStream",
                                        byteArrayOf(9),
                                        codec,
                                        codec,
                                        callOptions,
                                    )
                                var responses = 0
                                try {
                                    while (call.receive() != null) responses++
                                } finally {
                                    call.close()
                                }
                                assertEquals(128, responses)
                            }
                        }
                    }.awaitAll()
                }
            } finally {
                transport.shutdown()
                transportServer.shutdown()
                certificate.delete()
            }
        }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    fun `concurrent server shutdown callers await the same cleanup`() =
        runBlocking {
            val certificate = SelfSignedCertificate("localhost")
            val started = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val core = Server()
            core.routeUnary("test.Service", "Unary") { _, _ ->
                started.complete(Unit)
                release.await()
                ResponseEnvelope(byteArrayOf())
            }
            val transportServer = bindNative(core, certificate)
            val request = async { core.handleUnary(RpcRequest("test.Service", "Unary")) }
            started.await()
            val first = async(start = CoroutineStart.UNDISPATCHED) { transportServer.shutdown() }
            val second = async(start = CoroutineStart.UNDISPATCHED) { transportServer.shutdown() }
            try {
                assertFalse(second.isCompleted)
                release.complete(Unit)
                request.await()
                first.await()
                second.await()
            } finally {
                release.complete(Unit)
                runCatching { transportServer.shutdown() }
                certificate.delete()
            }
        }

    private suspend fun exerciseWebTransportAndConcurrentPost(config: NettyQuicClientConfig) {
        val connectionHandler =
            Http3ClientConnectionHandler(
                null,
                null,
                null,
                TrevRpcWebTransportServerConnectionHandler.webTransportSettings(),
                true,
                TrevRpcWebTransportServerConnectionHandler.webTransportSettingsValidator(),
            )
        val endpoint = connectQuic(config, HTTP3_ALPN, connectionHandler)
        val response = ConnectResponseHandler()
        val connectStream = Http3.newRequestStream(endpoint.quicChannel, response).awaitValue()
        val connectHeaders = DefaultHttp3HeadersFrame()
        connectHeaders
            .headers()
            .method("CONNECT")
            .protocol("webtransport")
            .scheme("https")
            .authority("${config.tls.serverName}:${config.remoteAddress.port}")
            .path(WEBTRANSPORT_PATH)
            .set("origin", "https://${config.tls.serverName}")
        connectStream.writeAndFlush(connectHeaders).awaitCompletion()
        assertEquals("draft02", response.awaitAccepted())

        val secondResponse = ConnectResponseHandler()
        val secondConnect = Http3.newRequestStream(endpoint.quicChannel, secondResponse).awaitValue()
        val secondHeaders = DefaultHttp3HeadersFrame()
        secondHeaders.headers().set(connectHeaders.headers())
        secondConnect.writeAndFlush(secondHeaders).awaitCompletion()
        assertEquals("409", secondResponse.awaitStatus().first)
        secondConnect.close().awaitCompletion()

        val rawInbox = RawFrameInbox(config.options)
        val rawStream =
            endpoint.quicChannel
                .newStreamBootstrap()
                .type(QuicStreamType.BIDIRECTIONAL)
                .handler(rawInbox.initializer())
                .create()
                .awaitValue()
        val prelude = rawStream.alloc().buffer(16)
        writeQuicVarint(prelude, TrevRpcWebTransportServerConnectionHandler.WEBTRANSPORT_BIDIRECTIONAL_STREAM_TYPE)
        writeQuicVarint(prelude, connectStream.streamId())
        rawStream.writeAndFlush(prelude).awaitCompletion()
        TrevRpcFrameWriter
            .write(
                rawStream,
                WireCodec.encode(RpcRequest("test.Service", "Unary", byteArrayOf(10))),
                config.options.maxFrameSize,
            ).awaitCompletion()
        rawStream.shutdownOutput().awaitCompletion()
        val rawResponse = WireCodec.decodeResponse(checkNotNull(rawInbox.receive()))
        assertEquals(listOf<Byte>(10, 1), rawResponse.body.toList())
        rawInbox.requireEnd(config.options.maxIdleTime)
        rawStream.close().awaitCompletion()

        val concurrentHttp3 = RawNettyHttp3RpcTransport.fromEndpoint(endpoint, config)
        try {
            val client = Client(concurrentHttp3)
            assertEquals(
                listOf<Byte>(11, 1),
                client
                    .unary(
                        "test.Service",
                        "Unary",
                        byteArrayOf(11),
                        MessageCodec.BYTE_ARRAY,
                        MessageCodec.BYTE_ARRAY,
                    ).toList(),
            )
            connectStream.close().awaitCompletion()
        } finally {
            concurrentHttp3.shutdown()
        }
    }

    private fun writeQuicVarint(
        output: ByteBuf,
        value: Long,
    ) {
        when {
            value <= 63 -> output.writeByte(value.toInt())
            value <= 16_383 -> output.writeShort((value or 0x4000).toInt())
            value <= 1_073_741_823 -> output.writeInt((value or 0x80000000L).toInt())
            else -> output.writeLong(value or 0xc000000000000000uL.toLong())
        }
    }

    private class ConnectResponseHandler : Http3RequestStreamInboundHandler() {
        private val response = CompletableDeferred<Pair<String, String>>()

        override fun channelRead(
            context: ChannelHandlerContext,
            frame: Http3HeadersFrame,
        ) {
            val status = frame.headers().status()?.toString()
            response.complete(
                status.orEmpty() to
                    frame.headers()["sec-webtransport-http3-draft"]?.toString().orEmpty(),
            )
        }

        override fun channelRead(
            context: ChannelHandlerContext,
            frame: Http3DataFrame,
        ) {
            frame.release()
        }

        override fun channelInputClosed(context: ChannelHandlerContext) {
            if (!response.isCompleted) {
                response.completeExceptionally(IllegalStateException("WebTransport CONNECT closed before response"))
            }
        }

        suspend fun awaitAccepted(): String {
            val (status, draft) = response.await()
            if (status != "200") throw IllegalStateException("WebTransport CONNECT failed: $status")
            return draft
        }

        suspend fun awaitStatus(): Pair<String, String> = response.await()
    }

    private suspend fun bindNative(
        core: Server,
        certificate: SelfSignedCertificate,
        options: NettyTransportOptions = NettyTransportOptions(),
    ): NettyRpcServer =
        NettyRpcServer.bind(
            core,
            NettyRpcServerConfig(
                bindAddress = InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                tls = NettyServerTls.Pem(certificate.privateKey(), certificate.certificate()),
                enableHttp3 = false,
                options = options,
            ),
        )

    private suspend fun connectNative(
        server: NettyRpcServer,
        certificate: SelfSignedCertificate,
        options: NettyTransportOptions = NettyTransportOptions(),
    ): RawNettyQuicRpcTransport =
        RawNettyQuicRpcTransport.connect(
            NettyQuicClientConfig(
                remoteAddress = server.localAddress,
                tls = NettyClientTls("localhost", trustCertificates = listOf(certificate.cert())),
                options = options,
            ),
        )

    private fun testServer(options: ServerOptions = ServerOptions()): Server =
        Server(options).apply {
            routeUnary("test.Service", "Unary", UnaryHandler { _, body -> ResponseEnvelope(body + byteArrayOf(1)) })
            routeServerStreaming(
                "test.Service",
                "ServerStream",
                ServerStreamingHandler { _, body ->
                    ResponseEnvelope(flowOf(body, body + byteArrayOf(2)))
                },
            )
            routeClientStreaming(
                "test.Service",
                "ClientStream",
                ClientStreamingHandler { _, requests ->
                    ResponseEnvelope(requests.toList().fold(byteArrayOf()) { total, next -> total + next })
                },
            )
            routeBidirectionalStreaming(
                "test.Service",
                "Bidi",
                BidirectionalStreamingHandler { _, requests ->
                    ResponseEnvelope(requests.map { it + byteArrayOf(3) })
                },
            )
        }

    private suspend fun assertCode(
        code: Code,
        block: suspend () -> Unit,
    ) {
        val error =
            try {
                block()
                throw AssertionError("expected TrevRpcException with code $code")
            } catch (caught: TrevRpcException) {
                caught
            }
        assertEquals(code, error.status.code)
    }

    private suspend fun exercise(transport: RpcTransport) {
        val client = Client(transport)
        val codec = MessageCodec.BYTE_ARRAY
        assertEquals(
            listOf<Byte>(9, 1),
            client.unary("test.Service", "Unary", byteArrayOf(9), codec, codec).toList(),
        )

        val serverStream =
            client.serverStreaming("test.Service", "ServerStream", byteArrayOf(4), codec, codec)
        assertEquals(listOf<Byte>(4), serverStream.receive()?.toList())
        assertEquals(listOf<Byte>(4, 2), serverStream.receive()?.toList())
        assertEquals(null, serverStream.receive())

        val clientStream = client.clientStreaming("test.Service", "ClientStream", codec, codec)
        clientStream.send(byteArrayOf(5))
        clientStream.send(byteArrayOf(6))
        assertEquals(listOf<Byte>(5, 6), clientStream.receive().message.toList())

        val bidi = client.bidirectionalStreaming("test.Service", "Bidi", codec, codec)
        bidi.send(byteArrayOf(7))
        bidi.send(byteArrayOf(8))
        bidi.closeSend()
        assertEquals(listOf<Byte>(7, 3), bidi.receive()?.toList())
        assertEquals(listOf<Byte>(8, 3), bidi.receive()?.toList())
        assertEquals(null, bidi.receive())
    }
}
