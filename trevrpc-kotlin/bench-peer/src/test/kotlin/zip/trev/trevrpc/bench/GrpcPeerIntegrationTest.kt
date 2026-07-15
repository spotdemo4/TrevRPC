package zip.trev.trevrpc.bench

import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusException
import kotlinx.coroutines.runBlocking
import org.bouncycastle.asn1.x509.GeneralName
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import zip.trev.trevrpc.benchmark.v1.BenchmarkRequest
import java.net.InetSocketAddress
import java.nio.file.Path
import java.util.concurrent.TimeUnit

class GrpcPeerIntegrationTest {
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    fun `grpc peer uses verified TLS and supports all RPC shapes on one channel`(): Unit =
        runBlocking {
            val authority = TestTlsAuthority.create()
            val untrustedAuthority = TestTlsAuthority.create()
            val identity = authority.issueServer(TEST_SERVER_IP, GeneralName.iPAddress)
            val wrongHost = authority.issueServer("localhost", GeneralName.dNSName)
            val server =
                GrpcBenchmarkServer.bind(
                    InetSocketAddress(TEST_SERVER_IP, 0),
                    identity.certificatePath,
                    identity.privateKeyPath,
                )
            val channel = createGrpcChannel(server.localAddress, authority.certificatePath)
            try {
                val client = GrpcBenchmarkClient(channel)
                for (kind in BenchmarkRpcKind.entries) {
                    val config = config(kind)
                    val counts = BenchmarkWorkload(client, config).runOperation()
                    assertEquals(kind.requestMessages(config.messagesPerStream), counts.requests, kind.wireName)
                    assertEquals(kind.responseMessages(config.messagesPerStream), counts.responses, kind.wireName)
                }

                val oversizedRequest =
                    BenchmarkRequest
                        .newBuilder()
                        .setPayload(ByteString.copyFrom(ByteArray(MAX_APPLICATION_PAYLOAD_BYTES + 1)))
                        .build()
                val limitError =
                    assertThrows(StatusException::class.java) {
                        runBlocking { client.unary(oversizedRequest) }
                    }
                assertEquals(Status.Code.INVALID_ARGUMENT, limitError.status.code)

                val untrustedChannel = createGrpcChannel(server.localAddress, untrustedAuthority.certificatePath)
                try {
                    assertThrows(Exception::class.java) {
                        runBlocking {
                            GrpcBenchmarkClient(untrustedChannel).unary(BenchmarkRequest.getDefaultInstance())
                        }
                    }
                } finally {
                    untrustedChannel.shutdownGracefully()
                }

                val wrongHostServer =
                    GrpcBenchmarkServer.bind(
                        InetSocketAddress(TEST_SERVER_IP, 0),
                        wrongHost.certificatePath,
                        wrongHost.privateKeyPath,
                    )
                val wrongHostChannel = createGrpcChannel(wrongHostServer.localAddress, authority.certificatePath)
                try {
                    assertThrows(Exception::class.java) {
                        runBlocking {
                            GrpcBenchmarkClient(wrongHostChannel).unary(BenchmarkRequest.getDefaultInstance())
                        }
                    }
                } finally {
                    wrongHostChannel.shutdownGracefully()
                    wrongHostServer.shutdown()
                }
            } finally {
                channel.shutdownGracefully()
                server.shutdown()
                authority.close()
                untrustedAuthority.close()
            }
        }

    private fun config(kind: BenchmarkRpcKind): PeerCommand.Client =
        PeerCommand.Client(
            stack = BenchmarkStack.GRPC_HTTP2,
            address = "127.0.0.1:7443",
            certificate = Path.of("unused"),
            rpcKind = kind,
            concurrency = 1,
            warmupMilliseconds = 0,
            measurementMilliseconds = 1,
            requestBytes = 17,
            responseBytes = 23,
            messagesPerStream = 35,
        )
}
