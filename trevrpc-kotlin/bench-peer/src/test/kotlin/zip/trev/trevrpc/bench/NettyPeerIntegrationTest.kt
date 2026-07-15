package zip.trev.trevrpc.bench

import io.netty.handler.ssl.util.SelfSignedCertificate
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import zip.trev.trevrpc.benchmark.v1.BenchmarkServiceClient
import zip.trev.trevrpc.netty.NettyClientTls
import zip.trev.trevrpc.netty.NettyQuicClientConfig
import zip.trev.trevrpc.netty.NettyRpcServer
import zip.trev.trevrpc.netty.NettyRpcServerConfig
import zip.trev.trevrpc.netty.NettyServerTls
import zip.trev.trevrpc.netty.NettyTransportOptions
import zip.trev.trevrpc.netty.advanced.RawNettyQuicRpcTransport
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.file.Path
import java.util.concurrent.TimeUnit

@Suppress("DEPRECATION")
class NettyPeerIntegrationTest {
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    fun `native peer accepts the pinned certificate and rejects another trust root`(): Unit =
        runBlocking {
            val identity = SelfSignedCertificate("localhost")
            val untrusted = SelfSignedCertificate("localhost")
            val server =
                NettyRpcServer.bind(
                    createBenchmarkServer(),
                    NettyRpcServerConfig(
                        bindAddress = InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                        tls = NettyServerTls.Pem(identity.privateKey(), identity.certificate()),
                        enableNative = true,
                        enableHttp3 = false,
                        options = transportOptions(),
                    ),
                )
            try {
                val trustedTransport =
                    RawNettyQuicRpcTransport.connect(
                        clientConfig(server.localAddress, identity.cert()),
                    )
                try {
                    for (kind in BenchmarkRpcKind.entries) {
                        val config = config(kind)
                        BenchmarkWorkload(
                            BenchmarkServiceClient(trustedTransport, benchmarkCallOptions(config)),
                            config,
                        ).runOperation()
                    }
                } finally {
                    trustedTransport.shutdown()
                }

                assertThrows(Exception::class.java) {
                    runBlocking {
                        RawNettyQuicRpcTransport.connect(
                            clientConfig(server.localAddress, untrusted.cert()),
                        )
                    }
                }
            } finally {
                server.shutdown()
                identity.delete()
                untrusted.delete()
            }
        }

    private fun clientConfig(
        address: InetSocketAddress,
        certificate: java.security.cert.X509Certificate,
    ): NettyQuicClientConfig =
        NettyQuicClientConfig(
            remoteAddress = address,
            tls = NettyClientTls("localhost", trustCertificates = listOf(certificate), verifyHostname = true),
            options = transportOptions(),
        )

    private fun transportOptions(): NettyTransportOptions =
        NettyTransportOptions(maxFrameSize = MAX_ENCODED_MESSAGE_BYTES, workerParallelism = 2)

    private fun config(kind: BenchmarkRpcKind): PeerCommand.Client =
        PeerCommand.Client(
            address = "127.0.0.1:7443",
            certificate = Path.of("unused"),
            rpcKind = kind,
            concurrency = 1,
            warmupMilliseconds = 0,
            measurementMilliseconds = 1,
            requestBytes = 8,
            responseBytes = 13,
            messagesPerStream = 35,
        )
}
