package zip.trev.trevrpc.netty

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.MultiThreadIoEventLoopGroup
import io.netty.channel.nio.NioIoHandler
import io.netty.channel.socket.nio.NioDatagramChannel
import io.netty.handler.codec.http3.DefaultHttp3DataFrame
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame
import io.netty.handler.codec.http3.Http3
import io.netty.handler.codec.http3.Http3ClientConnectionHandler
import io.netty.handler.codec.http3.Http3DataFrame
import io.netty.handler.codec.http3.Http3HeadersFrame
import io.netty.handler.codec.http3.Http3RequestStreamInboundHandler
import io.netty.handler.codec.http3.Http3ServerConnectionHandler
import io.netty.handler.codec.http3.TrevRpcWebTransportServerConnectionHandler
import io.netty.handler.codec.quic.InsecureQuicTokenHandler
import io.netty.handler.codec.quic.QuicChannel
import io.netty.handler.codec.quic.QuicServerCodecBuilder
import io.netty.handler.codec.quic.QuicStreamChannel
import io.netty.handler.codec.quic.QuicStreamType
import io.netty.handler.ssl.util.SelfSignedCertificate
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import zip.trev.trevrpc.BidirectionalStreamingHandler
import zip.trev.trevrpc.CallOptions
import zip.trev.trevrpc.Client
import zip.trev.trevrpc.ClientStreamingHandler
import zip.trev.trevrpc.Code
import zip.trev.trevrpc.MessageCodec
import zip.trev.trevrpc.Metadata
import zip.trev.trevrpc.ResponseEnvelope
import zip.trev.trevrpc.RpcKind
import zip.trev.trevrpc.RpcRequest
import zip.trev.trevrpc.RpcResponse
import zip.trev.trevrpc.RpcStreamFrame
import zip.trev.trevrpc.RpcTransport
import zip.trev.trevrpc.Server
import zip.trev.trevrpc.ServerOptions
import zip.trev.trevrpc.ServerStreamingHandler
import zip.trev.trevrpc.Status
import zip.trev.trevrpc.TrevRpcException
import zip.trev.trevrpc.UnaryHandler
import zip.trev.trevrpc.WireCodec
import zip.trev.trevrpc.netty.advanced.RawFrameInbox
import zip.trev.trevrpc.netty.advanced.RawNettyHttp3RpcTransport
import zip.trev.trevrpc.netty.advanced.RawNettyQuicRpcTransport
import zip.trev.trevrpc.netty.advanced.connectQuic
import zip.trev.trevrpc.readyResponseFlow
import java.net.InetAddress
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.measureTime

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
                withTimeout(2.seconds) { handlerStarted.await() }
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
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
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
            val transportOptions = NettyTransportOptions(workerParallelism = 2, maxIdleTime = 10.seconds)
            val transportServer = bindNative(core, certificate, transportOptions)
            val transport = connectNative(transportServer, certificate, transportOptions)
            try {
                val client = Client(transport)
                val codec = MessageCodec.BYTE_ARRAY
                val callOptions = CallOptions(maxResponseMessages = 128, streamIdleTimeout = 5.seconds)
                coroutineScope {
                    List(4) {
                        async {
                            repeat(250) {
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
    fun `client cancellation resets the native response stream and leaves the connection reusable`() =
        runBlocking {
            val certificate = SelfSignedCertificate("localhost")
            val started = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val finished = CompletableDeferred<Unit>()
            val core =
                Server(
                    ServerOptions(
                        gracefulShutdownTimeout = 200.milliseconds,
                        forceShutdownTimeout = 1.seconds,
                    ),
                )
            core.routeUnary("test.Service", "Blocked") { _, _ ->
                started.complete(Unit)
                try {
                    release.await()
                    ResponseEnvelope(byteArrayOf())
                } finally {
                    finished.complete(Unit)
                }
            }
            core.routeUnary("test.Service", "Echo") { _, body -> ResponseEnvelope(body) }
            val transportServer = bindNative(core, certificate)
            val transport = connectNative(transportServer, certificate)
            try {
                val call =
                    async {
                        Client(transport).unary(
                            "test.Service",
                            "Blocked",
                            byteArrayOf(),
                            MessageCodec.BYTE_ARRAY,
                            MessageCodec.BYTE_ARRAY,
                        )
                    }
                withTimeout(2.seconds) { started.await() }

                call.cancel(CancellationException("test cancellation"))
                runCatching { call.await() }

                assertFalse(finished.isCompleted)
                release.complete(Unit)
                withTimeout(10.seconds) { finished.await() }
                val echoed =
                    Client(transport).unary(
                        "test.Service",
                        "Echo",
                        byteArrayOf(1),
                        MessageCodec.BYTE_ARRAY,
                        MessageCodec.BYTE_ARRAY,
                    )
                assertArrayEquals(byteArrayOf(1), echoed)
            } finally {
                release.complete(Unit)
                runCatching { withTimeout(2.seconds) { transport.shutdown() } }
                runCatching { withTimeout(3.seconds) { transportServer.shutdown() } }
                certificate.delete()
            }
        }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    fun `client cancellation stops the HTTP3 response stream and leaves the connection reusable`() =
        runBlocking {
            val certificate = SelfSignedCertificate("localhost")
            val started = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val finished = CompletableDeferred<Unit>()
            val core =
                Server(
                    ServerOptions(
                        gracefulShutdownTimeout = 200.milliseconds,
                        forceShutdownTimeout = 1.seconds,
                    ),
                )
            core.routeUnary("test.Service", "Blocked") { _, _ ->
                started.complete(Unit)
                try {
                    release.await()
                    ResponseEnvelope(byteArrayOf())
                } finally {
                    finished.complete(Unit)
                }
            }
            core.routeUnary("test.Service", "Echo") { _, body -> ResponseEnvelope(body) }
            val transportServer =
                NettyRpcServer.bind(
                    core,
                    NettyRpcServerConfig(
                        bindAddress = InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                        tls = NettyServerTls.Pem(certificate.privateKey(), certificate.certificate()),
                    ),
                )
            val transport =
                RawNettyHttp3RpcTransport.connect(
                    NettyQuicClientConfig(
                        remoteAddress = transportServer.localAddress,
                        tls = NettyClientTls("localhost", trustCertificates = listOf(certificate.cert())),
                    ),
                )
            try {
                val call =
                    async {
                        Client(transport).unary(
                            "test.Service",
                            "Blocked",
                            byteArrayOf(),
                            MessageCodec.BYTE_ARRAY,
                            MessageCodec.BYTE_ARRAY,
                        )
                    }
                withTimeout(2.seconds) { started.await() }

                call.cancel(CancellationException("test cancellation"))
                runCatching { call.await() }

                assertFalse(finished.isCompleted)
                release.complete(Unit)
                withTimeout(10.seconds) { finished.await() }
                val echoed =
                    Client(transport).unary(
                        "test.Service",
                        "Echo",
                        byteArrayOf(1),
                        MessageCodec.BYTE_ARRAY,
                        MessageCodec.BYTE_ARRAY,
                    )
                assertArrayEquals(byteArrayOf(1), echoed)
            } finally {
                release.complete(Unit)
                runCatching { withTimeout(2.seconds) { transport.shutdown() } }
                runCatching { withTimeout(3.seconds) { transportServer.shutdown() } }
                certificate.delete()
            }
        }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    fun `connection loss cancels the exact native server job`() =
        runBlocking {
            withTimeout(10.seconds) {
                val certificate = SelfSignedCertificate("localhost")
                val started = CompletableDeferred<Unit>()
                val cancelled = CompletableDeferred<Unit>()
                val core =
                    Server(
                        ServerOptions(
                            gracefulShutdownTimeout = 200.milliseconds,
                            forceShutdownTimeout = 1.seconds,
                        ),
                    )
                core.routeUnary("test.Service", "Blocked") { _, _ ->
                    started.complete(Unit)
                    try {
                        awaitCancellation()
                    } finally {
                        cancelled.complete(Unit)
                    }
                }
                val transportServer = bindNative(core, certificate)
                val transport = connectNative(transportServer, certificate)
                val call =
                    async {
                        runCatching {
                            Client(transport).unary(
                                "test.Service",
                                "Blocked",
                                byteArrayOf(),
                                MessageCodec.BYTE_ARRAY,
                                MessageCodec.BYTE_ARRAY,
                            )
                        }
                    }
                try {
                    withTimeout(2.seconds) { started.await() }

                    withTimeout(2.seconds) { transport.shutdown() }

                    val failure = withTimeout(2.seconds) { call.await() }.exceptionOrNull()
                    assertTrue(failure is TrevRpcException)
                    assertEquals(Code.UNAVAILABLE, (failure as TrevRpcException).status.code)
                    withTimeout(2.seconds) { cancelled.await() }
                } finally {
                    call.cancel()
                    runCatching { withTimeout(1.seconds) { call.join() } }
                    runCatching { withTimeout(2.seconds) { transport.shutdown() } }
                    runCatching { withTimeout(3.seconds) { transportServer.shutdown() } }
                    certificate.delete()
                }
            }
        }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    fun `server force shutdown is bounded when a handler ignores cancellation`() =
        runBlocking {
            val certificate = SelfSignedCertificate("localhost")
            val started = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val core =
                Server(
                    ServerOptions(
                        gracefulShutdownTimeout = 50.milliseconds,
                        forceShutdownTimeout = 50.milliseconds,
                    ),
                )
            core.routeUnary("test.Service", "Blocked") { _, _ ->
                started.complete(Unit)
                withContext(NonCancellable) { release.await() }
                ResponseEnvelope(byteArrayOf())
            }
            val transportServer = bindNative(core, certificate)
            val transport = connectNative(transportServer, certificate)
            val call =
                async {
                    runCatching {
                        Client(transport).unary(
                            "test.Service",
                            "Blocked",
                            byteArrayOf(),
                            MessageCodec.BYTE_ARRAY,
                            MessageCodec.BYTE_ARRAY,
                        )
                    }
                }
            try {
                withTimeout(2.seconds) { started.await() }
                lateinit var firstFailure: Throwable
                lateinit var secondFailure: Throwable
                val elapsed =
                    measureTime {
                        val first = async { runCatching { transportServer.shutdown() }.exceptionOrNull() }
                        val second = async { runCatching { transportServer.shutdown() }.exceptionOrNull() }
                        firstFailure = checkNotNull(first.await())
                        secondFailure = checkNotNull(second.await())
                    }

                assertTrue(firstFailure is IllegalStateException)
                assertEquals(firstFailure.message, secondFailure.message)
                assertTrue(elapsed < 2.seconds)
            } finally {
                release.complete(Unit)
                call.await()
                runCatching { transport.shutdown() }
                runCatching { transportServer.shutdown() }
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
            withTimeout(2.seconds) { started.await() }
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

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    fun `remote non-OK terminal survives missing FIN on native and HTTP3 unary and streaming paths`() =
        runBlocking {
            for (http3 in listOf(false, true)) {
                for (unary in listOf(true, false)) {
                    for (connectionLoss in listOf(false, true)) {
                        assertRemoteFailureWithoutFin(http3, unary, connectionLoss)
                    }
                }
            }
        }

    private suspend fun assertRemoteFailureWithoutFin(
        http3: Boolean,
        unary: Boolean,
        connectionLoss: Boolean,
    ) {
        val certificate = SelfSignedCertificate("localhost")
        val metadata = Metadata.of("failure-detail" to byteArrayOf(4, 5, 6))
        val status = Status.notFound("remote terminal status").withMetadata(metadata)
        val encoded =
            if (unary) {
                WireCodec.encode(RpcResponse.fromStatus(status))
            } else {
                WireCodec.encode(RpcStreamFrame.status(status))
            }
        val peer = ScriptedTerminalServer.bind(certificate, http3, encoded, connectionLoss)
        val options = NettyTransportOptions(maxIdleTime = 20.milliseconds, shutdownTimeout = 1.seconds)
        val config =
            NettyQuicClientConfig(
                remoteAddress = peer.localAddress,
                tls = NettyClientTls("localhost", trustCertificates = listOf(certificate.cert())),
                options = options,
            )
        lateinit var shutdown: suspend () -> Unit
        val transport: RpcTransport =
            if (http3) {
                RawNettyHttp3RpcTransport.connect(config).also { shutdown = it::shutdown }
            } else {
                RawNettyQuicRpcTransport.connect(config).also { shutdown = it::shutdown }
            }
        try {
            val actual =
                if (unary) {
                    transport.unary(RpcRequest("test.Service", "Failure")).status
                } else {
                    checkNotNull(
                        transport
                            .openStream(
                                RpcRequest(
                                    "test.Service",
                                    "Failure",
                                    kindValue = RpcKind.SERVER_STREAMING.value,
                                ),
                            ).receive(),
                    ).status
                }
            assertEquals(status.code, actual.code)
            assertEquals(status.message, actual.message)
            assertEquals(status.metadata, actual.metadata)
        } finally {
            runCatching { shutdown() }
            peer.close()
            certificate.delete()
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    fun `native server stream survives immediate peer receive retirement`(): Unit =
        runBlocking {
            val certificate = SelfSignedCertificate("localhost")
            val peer =
                ScriptedTerminalServer.bind(
                    certificate,
                    http3 = false,
                    finishResponse = true,
                    abortReceiveAfterRequest = true,
                )
            val config =
                NettyQuicClientConfig(
                    remoteAddress = peer.localAddress,
                    tls = NettyClientTls("localhost", trustCertificates = listOf(certificate.cert())),
                    options = NettyTransportOptions(shutdownTimeout = 1.seconds),
                )
            val transport = RawNettyQuicRpcTransport.connect(config)
            try {
                val call =
                    withTimeout(2.seconds) {
                        Client(transport)
                            .serverStreaming(
                                "test.Service",
                                "FastServerStream",
                                byteArrayOf(1, 2, 3),
                                MessageCodec.BYTE_ARRAY,
                                MessageCodec.BYTE_ARRAY,
                            )
                    }
                assertEquals(null, withTimeout(2.seconds) { call.receive() })
                assertEquals(1, peer.receivedRequestFrames)
            } finally {
                runCatching { transport.shutdown() }
                peer.close()
                certificate.delete()
            }
        }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    fun `native queued responses do not repeat an atomically submitted request FIN`(): Unit =
        runBlocking {
            val certificate = SelfSignedCertificate("localhost")
            val response = WireCodec.encode(RpcStreamFrame.message(byteArrayOf(9)))
            val terminal = WireCodec.encode(RpcStreamFrame.status(Status.ok()))
            val responses = List(32) { response } + terminal
            val peer =
                ScriptedTerminalServer.bind(
                    certificate,
                    http3 = false,
                    finishResponse = true,
                    abortReceiveAfterRequest = true,
                    queuedResponseBodies = responses,
                )
            val config =
                NettyQuicClientConfig(
                    remoteAddress = peer.localAddress,
                    tls = NettyClientTls("localhost", trustCertificates = listOf(certificate.cert())),
                    options = NettyTransportOptions(shutdownTimeout = 1.seconds),
                )
            val transport = RawNettyQuicRpcTransport.connect(config)
            try {
                coroutineScope {
                    List(8) {
                        async {
                            repeat(128) {
                                val call =
                                    Client(transport)
                                        .serverStreaming(
                                            "test.Service",
                                            "QueuedServerStream",
                                            byteArrayOf(1),
                                            MessageCodec.BYTE_ARRAY,
                                            MessageCodec.BYTE_ARRAY,
                                            CallOptions(timeout = 5.seconds, maxResponseMessages = 32),
                                        )
                                var received = 0
                                try {
                                    while (call.receive() != null) received++
                                } finally {
                                    call.close()
                                }
                                assertEquals(32, received)
                            }
                        }
                    }.awaitAll()
                }
                assertEquals(1_024, peer.receivedRequestFrames)
            } finally {
                runCatching { transport.shutdown() }
                peer.close()
                certificate.delete()
            }
        }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    fun `native close unblocks terminal response waiting for FIN`(): Unit =
        runBlocking {
            assertTerminalWithoutFinClose(http3 = false)
        }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    fun `HTTP3 close unblocks terminal response waiting for FIN`(): Unit =
        runBlocking {
            assertTerminalWithoutFinClose(http3 = true)
        }

    private suspend fun assertTerminalWithoutFinClose(http3: Boolean) =
        coroutineScope {
            val certificate = SelfSignedCertificate("localhost")
            val peer = ScriptedTerminalServer.bind(certificate, http3)
            val terminalObserved = CompletableDeferred<Unit>()
            val options = NettyTransportOptions(maxIdleTime = 10.seconds, shutdownTimeout = 1.seconds)
            val config =
                NettyQuicClientConfig(
                    remoteAddress = peer.localAddress,
                    tls = NettyClientTls("localhost", trustCertificates = listOf(certificate.cert())),
                    options = options,
                )
            lateinit var shutdown: suspend () -> Unit
            val transport: RpcTransport =
                if (http3) {
                    RawNettyHttp3RpcTransport.connect(config) { terminalObserved.complete(Unit) }.also { client ->
                        shutdown = client::shutdown
                    }
                } else {
                    RawNettyQuicRpcTransport.connect(config) { terminalObserved.complete(Unit) }.also { client ->
                        shutdown = client::shutdown
                    }
                }
            val stream =
                transport.openStream(
                    RpcRequest(
                        "test.Service",
                        "NeverFinishes",
                        kindValue = RpcKind.SERVER_STREAMING.value,
                    ),
                )
            val receiving = async { runCatching { stream.receive() } }
            try {
                withTimeout(2.seconds) { terminalObserved.await() }
                assertFalse(receiving.isCompleted)
                val elapsed = measureTime { withTimeout(2.seconds) { stream.close() } }
                assertTrue(elapsed < 2.seconds)
                withTimeout(2.seconds) { receiving.await() }
            } finally {
                receiving.cancel()
                runCatching { shutdown() }
                peer.close()
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
                    ResponseEnvelope(
                        readyResponseFlow {
                            emit(body)
                            emit(body + byteArrayOf(2))
                        },
                    )
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

private class ScriptedTerminalServer private constructor(
    private val group: MultiThreadIoEventLoopGroup,
    private val datagram: Channel,
    private val connections: MutableSet<QuicChannel>,
    private val requestFrames: AtomicInteger,
) {
    val localAddress: InetSocketAddress
        get() = datagram.localAddress() as InetSocketAddress

    val receivedRequestFrames: Int
        get() = requestFrames.get()

    suspend fun close() {
        connections.toList().forEach { it.closeApplication(0, "test complete") }
        datagram.close().awaitCompletion()
        group.shutdownNow(2.seconds)
    }

    companion object {
        suspend fun bind(
            certificate: SelfSignedCertificate,
            http3: Boolean,
            responseBody: ByteArray = WireCodec.encode(RpcStreamFrame.status(Status.ok())),
            closeConnectionAfterResponse: Boolean = false,
            finishResponse: Boolean = false,
            abortReceiveAfterRequest: Boolean = false,
            queuedResponseBodies: List<ByteArray> = emptyList(),
        ): ScriptedTerminalServer {
            require(!http3 || (!finishResponse && !abortReceiveAfterRequest && queuedResponseBodies.isEmpty()))
            require(queuedResponseBodies.isEmpty() || finishResponse)
            val group = MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory())
            val connections = ConcurrentHashMap.newKeySet<QuicChannel>()
            val requestFrames = AtomicInteger()
            val protocol = if (http3) HTTP3_ALPN else zip.trev.trevrpc.ALPN
            val ssl =
                io.netty.handler.codec.quic.QuicSslContextBuilder
                    .forServer(certificate.privateKey(), null, certificate.certificate())
                    .applicationProtocols(protocol)
                    .build()
            val streamInitializer =
                if (http3) {
                    http3TerminalStreamInitializer(responseBody, closeConnectionAfterResponse)
                } else {
                    nativeTerminalStreamInitializer(
                        responseBody,
                        closeConnectionAfterResponse,
                        finishResponse,
                        abortReceiveAfterRequest,
                        queuedResponseBodies,
                        requestFrames,
                    )
                }
            val connectionInitializer =
                object : ChannelInitializer<QuicChannel>() {
                    override fun initChannel(channel: QuicChannel) {
                        connections += channel
                        channel.closeFuture().addListener { connections -= channel }
                        if (http3) channel.pipeline().addLast(Http3ServerConnectionHandler(streamInitializer))
                    }
                }
            val builder: QuicServerCodecBuilder =
                if (http3) Http3.newQuicServerCodecBuilder() else QuicServerCodecBuilder()
            val codec =
                builder
                    .sslContext(ssl)
                    .maxIdleTimeout(30_000, TimeUnit.MILLISECONDS)
                    .initialMaxData(1_048_576)
                    .initialMaxStreamDataBidirectionalRemote(1_048_576)
                    .initialMaxStreamDataBidirectionalLocal(1_048_576)
                    .initialMaxStreamsBidirectional(64)
                    .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
                    .handler(connectionInitializer)
                    .streamHandler(
                        if (http3) {
                            object : ChannelInitializer<QuicStreamChannel>() {
                                override fun initChannel(channel: QuicStreamChannel) = Unit
                            }
                        } else {
                            streamInitializer
                        },
                    ).build()
            val datagram =
                Bootstrap()
                    .group(group)
                    .channel(NioDatagramChannel::class.java)
                    .handler(codec)
                    .bind(InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
                    .awaitChannel()
            return ScriptedTerminalServer(group, datagram, connections, requestFrames)
        }

        private fun nativeTerminalStreamInitializer(
            responseBody: ByteArray,
            closeConnectionAfterResponse: Boolean,
            finishResponse: Boolean,
            abortReceiveAfterRequest: Boolean,
            queuedResponseBodies: List<ByteArray>,
            requestFrames: AtomicInteger,
        ): ChannelInitializer<QuicStreamChannel> =
            object : ChannelInitializer<QuicStreamChannel>() {
                override fun initChannel(channel: QuicStreamChannel) {
                    channel.pipeline().addLast(
                        TrevRpcFrameDecoder(zip.trev.trevrpc.DEFAULT_MAX_FRAME_SIZE),
                        object : ChannelInboundHandlerAdapter() {
                            private var responded = false

                            override fun channelRead(
                                context: ChannelHandlerContext,
                                message: Any,
                            ) {
                                (message as ByteBuf).release()
                                requestFrames.incrementAndGet()
                                if (responded) return
                                responded = true
                                val stream = context.channel() as QuicStreamChannel
                                if (abortReceiveAfterRequest) stream.shutdownInput(0)
                                val responseBodies = queuedResponseBodies.ifEmpty { listOf(responseBody) }
                                responseBodies.dropLast(1).forEach { body ->
                                    TrevRpcFrameWriter.write(
                                        stream,
                                        body,
                                        zip.trev.trevrpc.DEFAULT_MAX_FRAME_SIZE,
                                    )
                                }
                                val write =
                                    if (finishResponse) {
                                        TrevRpcFrameWriter.writeFinal(
                                            stream,
                                            responseBodies.last(),
                                            zip.trev.trevrpc.DEFAULT_MAX_FRAME_SIZE,
                                        )
                                    } else {
                                        TrevRpcFrameWriter.write(
                                            stream,
                                            responseBodies.last(),
                                            zip.trev.trevrpc.DEFAULT_MAX_FRAME_SIZE,
                                        )
                                    }
                                if (closeConnectionAfterResponse) {
                                    write.addListener {
                                        val connection = (context.channel() as QuicStreamChannel).parent()
                                        connection.closeApplication(1, "test connection loss")
                                        connection.close()
                                    }
                                }
                            }
                        },
                    )
                }
            }

        private fun http3TerminalStreamInitializer(
            responseBody: ByteArray,
            closeConnectionAfterResponse: Boolean,
        ): ChannelInitializer<QuicStreamChannel> =
            object : ChannelInitializer<QuicStreamChannel>() {
                override fun initChannel(channel: QuicStreamChannel) {
                    channel.pipeline().addLast(
                        object : Http3RequestStreamInboundHandler() {
                            private var headersSent = false
                            private var responded = false

                            override fun channelRead(
                                context: ChannelHandlerContext,
                                frame: Http3HeadersFrame,
                            ) {
                                if (headersSent) return
                                headersSent = true
                                context.writeAndFlush(
                                    DefaultHttp3HeadersFrame().apply {
                                        headers().status("200").set("content-type", TREV_RPC_MEDIA_TYPE)
                                    },
                                )
                            }

                            override fun channelRead(
                                context: ChannelHandlerContext,
                                frame: Http3DataFrame,
                            ) {
                                frame.release()
                                if (responded) return
                                responded = true
                                val payload =
                                    TrevRpcFrameWriter.encode(
                                        context.alloc(),
                                        responseBody,
                                        zip.trev.trevrpc.DEFAULT_MAX_FRAME_SIZE,
                                    )
                                val write = context.writeAndFlush(DefaultHttp3DataFrame(payload))
                                if (closeConnectionAfterResponse) {
                                    write.addListener {
                                        val connection = (context.channel() as QuicStreamChannel).parent()
                                        connection.closeApplication(1, "test connection loss")
                                        connection.close()
                                    }
                                }
                            }

                            override fun channelInputClosed(context: ChannelHandlerContext) = Unit
                        },
                    )
                }
            }
    }
}
