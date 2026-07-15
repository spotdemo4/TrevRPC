package zip.trev.trevrpc.bench

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import zip.trev.trevrpc.RequestContext
import zip.trev.trevrpc.Server
import zip.trev.trevrpc.ServerOptions
import zip.trev.trevrpc.Status
import zip.trev.trevrpc.TrevRpcException
import zip.trev.trevrpc.benchmark.v1.BenchmarkRequest
import zip.trev.trevrpc.benchmark.v1.BenchmarkResponse
import zip.trev.trevrpc.benchmark.v1.BenchmarkServiceService
import zip.trev.trevrpc.benchmark.v1.BenchmarkSummary
import zip.trev.trevrpc.benchmark.v1.StreamRequest
import zip.trev.trevrpc.benchmark.v1.registerBenchmarkService
import zip.trev.trevrpc.readyResponseFlow

internal class BenchmarkRpcService : BenchmarkServiceService {
    override suspend fun unary(
        context: RequestContext,
        request: BenchmarkRequest,
    ): BenchmarkResponse = response(request.sequence, checkedResponseBytes(request.responseBytes))

    override suspend fun clientStream(
        context: RequestContext,
        requests: Flow<BenchmarkRequest>,
    ): BenchmarkSummary {
        var messageCount = 0L
        var payloadBytes = 0L
        requests.collect { request ->
            messageCount = Math.addExact(messageCount, 1L)
            payloadBytes = Math.addExact(payloadBytes, request.payload.size().toLong())
        }
        return BenchmarkSummary
            .newBuilder()
            .setMessageCount(messageCount)
            .setPayloadBytes(payloadBytes)
            .build()
    }

    override suspend fun serverStream(
        context: RequestContext,
        request: StreamRequest,
    ): Flow<BenchmarkResponse> {
        val messageCount = checkedMessageCount(request.messageCount)
        val responseBytes = checkedResponseBytes(request.responseBytes)
        return readyResponseFlow {
            repeat(messageCount) { sequence -> emit(response(sequence.toLong(), responseBytes)) }
        }
    }

    override suspend fun bidi(
        context: RequestContext,
        requests: Flow<BenchmarkRequest>,
    ): Flow<BenchmarkResponse> = requests.map { request -> response(request.sequence, checkedResponseBytes(request.responseBytes)) }

    private fun response(
        sequence: Long,
        responseBytes: Int,
    ): BenchmarkResponse =
        BenchmarkResponse
            .newBuilder()
            .setSequence(sequence)
            .setPayload(ByteString.copyFrom(ByteArray(responseBytes)))
            .build()

    private fun checkedResponseBytes(responseBytes: Int): Int {
        if (responseBytes !in 0..MAX_APPLICATION_PAYLOAD_BYTES) {
            throw TrevRpcException(Status.invalidArgument("response_bytes is outside the benchmark peer limit"))
        }
        return responseBytes
    }

    private fun checkedMessageCount(messageCount: Int): Int {
        if (messageCount !in 1..MAX_MESSAGES_PER_STREAM) {
            throw TrevRpcException(Status.invalidArgument("message_count is outside the benchmark peer limit"))
        }
        return messageCount
    }
}

internal fun createBenchmarkServer(): Server {
    val server =
        Server(
            ServerOptions(
                maxFrameSize = MAX_ENCODED_MESSAGE_BYTES,
                maxConcurrentConnections = 1,
                maxConcurrentStreamsPerConnection = MAX_BENCHMARK_CONCURRENCY,
                maxConcurrentRequests = MAX_BENCHMARK_CONCURRENCY,
                maxStreamMessages = MAX_MESSAGES_PER_STREAM,
                maxStreamBodySize = null,
            ),
        )
    registerBenchmarkService(server, BenchmarkRpcService())
    return server
}
