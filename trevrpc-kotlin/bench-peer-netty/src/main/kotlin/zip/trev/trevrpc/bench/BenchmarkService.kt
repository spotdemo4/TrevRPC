package zip.trev.trevrpc.bench

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import zip.trev.trevrpc.RequestContext
import zip.trev.trevrpc.ResponseEnvelope
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
    private val application = BenchmarkApplication { message -> TrevRpcException(Status.invalidArgument(message)) }

    override suspend fun unary(
        context: RequestContext,
        request: BenchmarkRequest,
    ): ResponseEnvelope<BenchmarkResponse> = ResponseEnvelope(application.unary(request))

    override suspend fun clientStream(
        context: RequestContext,
        requests: Flow<BenchmarkRequest>,
    ): ResponseEnvelope<BenchmarkSummary> = ResponseEnvelope(application.clientStream(requests))

    override suspend fun serverStream(
        context: RequestContext,
        request: StreamRequest,
    ): ResponseEnvelope<Flow<BenchmarkResponse>> = ResponseEnvelope(application.serverStream(request))

    override suspend fun bidi(
        context: RequestContext,
        requests: Flow<BenchmarkRequest>,
    ): ResponseEnvelope<Flow<BenchmarkResponse>> = ResponseEnvelope(application.bidi(requests))
}

internal class BenchmarkApplication(
    private val invalidArgument: (String) -> Throwable,
) {
    @Volatile
    private var cachedResponsePayload: ByteString? = null

    fun unary(request: BenchmarkRequest): BenchmarkResponse {
        checkRequestPayload(request.payload)
        return response(request.sequence, checkedResponseBytes(request.responseBytes))
    }

    suspend fun clientStream(requests: Flow<BenchmarkRequest>): BenchmarkSummary {
        var messageCount = 0L
        var payloadBytes = 0L
        requests.collect { request ->
            checkRequestPayload(request.payload)
            messageCount = Math.addExact(messageCount, 1L)
            checkRequestMessageCount(messageCount)
            payloadBytes = Math.addExact(payloadBytes, request.payload.size().toLong())
        }
        return BenchmarkSummary
            .newBuilder()
            .setMessageCount(messageCount)
            .setPayloadBytes(payloadBytes)
            .build()
    }

    fun serverStream(request: StreamRequest): Flow<BenchmarkResponse> {
        checkRequestPayload(request.payload)
        val messageCount = checkedMessageCount(request.messageCount)
        val responseBytes = checkedResponseBytes(request.responseBytes)
        return readyResponseFlow {
            repeat(messageCount) { sequence -> emit(response(sequence.toLong(), responseBytes)) }
        }
    }

    fun bidi(requests: Flow<BenchmarkRequest>): Flow<BenchmarkResponse> =
        flow {
            var messageCount = 0L
            requests.collect { request ->
                checkRequestPayload(request.payload)
                messageCount++
                checkRequestMessageCount(messageCount)
                emit(response(request.sequence, checkedResponseBytes(request.responseBytes)))
            }
        }

    private fun response(
        sequence: Long,
        responseBytes: Int,
    ): BenchmarkResponse =
        BenchmarkResponse
            .newBuilder()
            .setSequence(sequence)
            .setPayload(responsePayload(responseBytes))
            .build()

    private fun responsePayload(responseBytes: Int): ByteString {
        val cached = cachedResponsePayload
        if (cached != null) {
            return if (cached.size() == responseBytes) cached else ByteString.copyFrom(ByteArray(responseBytes))
        }
        val created = ByteString.copyFrom(ByteArray(responseBytes))
        return synchronized(this) {
            val published = cachedResponsePayload
            when {
                published == null -> created.also { cachedResponsePayload = it }
                published.size() == responseBytes -> published
                else -> created
            }
        }
    }

    private fun checkedResponseBytes(responseBytes: Int): Int {
        if (responseBytes !in 0..MAX_APPLICATION_PAYLOAD_BYTES) {
            throw invalidArgument("response_bytes is outside the benchmark peer limit")
        }
        return responseBytes
    }

    private fun checkedMessageCount(messageCount: Int): Int {
        if (messageCount !in 1..MAX_MESSAGES_PER_STREAM) {
            throw invalidArgument("message_count is outside the benchmark peer limit")
        }
        return messageCount
    }

    private fun checkRequestPayload(payload: ByteString) {
        if (payload.size() > MAX_APPLICATION_PAYLOAD_BYTES) {
            throw invalidArgument("request payload is outside the benchmark peer limit")
        }
    }

    private fun checkRequestMessageCount(messageCount: Long) {
        if (messageCount > MAX_MESSAGES_PER_STREAM) {
            throw invalidArgument("request stream exceeds the benchmark peer message limit")
        }
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
