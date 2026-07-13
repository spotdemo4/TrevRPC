package zip.trev.trevrpc.bench

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import zip.trev.trevrpc.RpcRequest
import zip.trev.trevrpc.RpcResponse
import zip.trev.trevrpc.RpcTransport
import zip.trev.trevrpc.RpcTransportStream
import zip.trev.trevrpc.Server
import zip.trev.trevrpc.benchmark.v1.BenchmarkServiceClient
import java.nio.file.Path

class WorkloadTest {
    @Test
    fun `all four generated RPC shapes validate messages and counts`() =
        runBlocking {
            for (kind in BenchmarkRpcKind.entries) {
                val config = config(kind)
                val server = createBenchmarkServer()
                try {
                    val client = BenchmarkServiceClient(InMemoryTransport(server), benchmarkCallOptions(config))
                    val counts = BenchmarkWorkload(client, config).runOperation()
                    assertEquals(kind.requestMessages(config.messagesPerStream), counts.requests, kind.wireName)
                    assertEquals(kind.responseMessages(config.messagesPerStream), counts.responses, kind.wireName)
                } finally {
                    server.shutdown()
                }
            }
        }

    private fun config(kind: BenchmarkRpcKind): PeerCommand.Client =
        PeerCommand.Client(
            address = "127.0.0.1:7443",
            certificate = Path.of("unused"),
            rpcKind = kind,
            concurrency = 1,
            warmupMilliseconds = 0,
            measurementMilliseconds = 1,
            requestBytes = 17,
            responseBytes = 23,
            messagesPerStream = 5,
        )

    private class InMemoryTransport(
        private val server: Server,
    ) : RpcTransport {
        override suspend fun unary(request: RpcRequest): RpcResponse = server.handleUnary(request)

        override suspend fun openStream(
            request: RpcRequest,
            requestBody: Flow<ByteArray>,
        ): RpcTransportStream = server.handleStreaming(request, requestBody)
    }
}
