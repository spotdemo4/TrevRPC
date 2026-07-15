package zip.trev.trevrpc.bench

import kotlinx.coroutines.runBlocking
import org.bouncycastle.asn1.x509.GeneralName
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
import java.net.InetSocketAddress
import java.nio.file.Path
import java.util.concurrent.TimeUnit

class NettyPeerIntegrationTest {
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    fun `native peer verifies trust and requested IP SAN`(): Unit =
        runBlocking {
            val authority = TestTlsAuthority.create()
            val untrustedAuthority = TestTlsAuthority.create()
            val identity = authority.issueServer(TEST_SERVER_IP, GeneralName.iPAddress)
            val wrongHost = authority.issueServer("localhost", GeneralName.dNSName)
            val server =
                NettyRpcServer.bind(
                    createBenchmarkServer(),
                    NettyRpcServerConfig(
                        bindAddress = InetSocketAddress(TEST_SERVER_IP, 0),
                        tls = NettyServerTls.KeyAndCertificates(identity.privateKey, identity.certificateChain),
                        enableNative = true,
                        enableHttp3 = false,
                        options = transportOptions(),
                    ),
                )
            try {
                val trustedTransport =
                    RawNettyQuicRpcTransport.connect(
                        clientConfig(server.localAddress, authority.certificate),
                    )
                try {
                    for (kind in BenchmarkRpcKind.entries) {
                        val config = config(kind)
                        BenchmarkWorkload(
                            NativeBenchmarkClient(BenchmarkServiceClient(trustedTransport, benchmarkCallOptions(config))),
                            config,
                        ).runOperation()
                    }
                } finally {
                    trustedTransport.shutdown()
                }

                assertThrows(Exception::class.java) {
                    runBlocking {
                        RawNettyQuicRpcTransport.connect(
                            clientConfig(server.localAddress, untrustedAuthority.certificate),
                        )
                    }
                }

                val wrongHostServer =
                    NettyRpcServer.bind(
                        createBenchmarkServer(),
                        NettyRpcServerConfig(
                            bindAddress = InetSocketAddress(TEST_SERVER_IP, 0),
                            tls = NettyServerTls.KeyAndCertificates(wrongHost.privateKey, wrongHost.certificateChain),
                            enableNative = true,
                            enableHttp3 = false,
                            options = transportOptions(),
                        ),
                    )
                try {
                    assertThrows(Exception::class.java) {
                        runBlocking {
                            RawNettyQuicRpcTransport.connect(
                                clientConfig(wrongHostServer.localAddress, authority.certificate),
                            )
                        }
                    }
                } finally {
                    wrongHostServer.shutdown()
                }
            } finally {
                server.shutdown()
                authority.close()
                untrustedAuthority.close()
            }
        }

    private fun clientConfig(
        address: InetSocketAddress,
        certificate: java.security.cert.X509Certificate,
    ): NettyQuicClientConfig =
        NettyQuicClientConfig(
            remoteAddress = address,
            tls = NettyClientTls(address.hostString, trustCertificates = listOf(certificate), verifyHostname = true),
            options = transportOptions(),
        )

    private fun transportOptions(): NettyTransportOptions =
        NettyTransportOptions(maxFrameSize = MAX_ENCODED_MESSAGE_BYTES, workerParallelism = 2)

    private fun config(kind: BenchmarkRpcKind): PeerCommand.Client =
        PeerCommand.Client(
            stack = BenchmarkStack.TREVRPC_NATIVE_QUIC,
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
