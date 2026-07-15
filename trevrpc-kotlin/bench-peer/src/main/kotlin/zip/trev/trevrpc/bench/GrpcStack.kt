package zip.trev.trevrpc.bench

import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.Status
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.withContext
import zip.trev.trevrpc.benchmark.v1.BenchmarkRequest
import zip.trev.trevrpc.benchmark.v1.BenchmarkResponse
import zip.trev.trevrpc.benchmark.v1.BenchmarkServiceGrpcKt
import zip.trev.trevrpc.benchmark.v1.BenchmarkSummary
import zip.trev.trevrpc.benchmark.v1.StreamRequest
import java.net.InetSocketAddress
import java.nio.file.Path
import java.util.concurrent.TimeUnit

internal class GrpcBenchmarkService : BenchmarkServiceGrpcKt.BenchmarkServiceCoroutineImplBase() {
    private val application =
        BenchmarkApplication { message -> Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException() }

    override suspend fun unary(request: BenchmarkRequest): BenchmarkResponse = application.unary(request)

    override suspend fun clientStream(requests: Flow<BenchmarkRequest>): BenchmarkSummary = application.clientStream(requests)

    override fun serverStream(request: StreamRequest): Flow<BenchmarkResponse> = application.serverStream(request)

    override fun bidi(requests: Flow<BenchmarkRequest>): Flow<BenchmarkResponse> = application.bidi(requests)
}

internal class GrpcBenchmarkClient(
    channel: ManagedChannel,
) : BenchmarkClient {
    private val stub = BenchmarkServiceGrpcKt.BenchmarkServiceCoroutineStub(channel)

    override suspend fun unary(request: BenchmarkRequest): BenchmarkResponse = stub.unary(request)

    override suspend fun clientStream(requests: Flow<List<BenchmarkRequest>>): BenchmarkSummary =
        stub.clientStream(requests.flattenBatches())

    override fun serverStream(request: StreamRequest): Flow<BenchmarkResponse> = stub.serverStream(request)

    override fun bidi(requests: Flow<List<BenchmarkRequest>>): Flow<BenchmarkResponse> = stub.bidi(requests.flattenBatches())
}

internal class GrpcBenchmarkServer private constructor(
    private val server: Server,
    val localAddress: InetSocketAddress,
) {
    suspend fun shutdown() {
        server.shutdown()
        val terminated = withContext(Dispatchers.IO) { server.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS) }
        if (!terminated) {
            server.shutdownNow()
            withContext(Dispatchers.IO) { server.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS) }
        }
    }

    companion object {
        fun bind(
            address: InetSocketAddress,
            certificate: Path,
            privateKey: Path,
        ): GrpcBenchmarkServer {
            val sslContext =
                GrpcSslContexts
                    .forServer(certificate.toFile(), privateKey.toFile())
                    .build()
            val server =
                NettyServerBuilder
                    .forAddress(address)
                    .sslContext(sslContext)
                    .maxInboundMessageSize(MAX_ENCODED_MESSAGE_BYTES)
                    .maxConcurrentCallsPerConnection(MAX_BENCHMARK_CONCURRENCY)
                    .addService(GrpcBenchmarkService())
                    .build()
                    .start()
            return GrpcBenchmarkServer(server, InetSocketAddress(address.address, server.port))
        }
    }
}

internal fun createGrpcChannel(
    address: InetSocketAddress,
    certificate: Path,
): ManagedChannel {
    val sslContext =
        GrpcSslContexts
            .forClient()
            .trustManager(certificate.toFile())
            .build()
    return NettyChannelBuilder
        .forAddress(address)
        .sslContext(sslContext)
        .maxInboundMessageSize(MAX_ENCODED_MESSAGE_BYTES)
        .disableRetry()
        .build()
}

internal suspend fun ManagedChannel.shutdownGracefully() {
    shutdown()
    val terminated = withContext(Dispatchers.IO) { awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS) }
    if (!terminated) {
        shutdownNow()
        withContext(Dispatchers.IO) { awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS) }
    }
}

private fun <T> Flow<List<T>>.flattenBatches(): Flow<T> =
    flow {
        collect { batch -> batch.forEach { emit(it) } }
    }

private const val SHUTDOWN_TIMEOUT_SECONDS = 5L
