package zip.trev.trevrpc.benchmark.support

import com.google.protobuf.ByteString
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import zip.trev.trevrpc.benchmark.v1.BenchmarkRequest
import zip.trev.trevrpc.benchmark.v1.BenchmarkResponse
import zip.trev.trevrpc.benchmark.v1.BenchmarkServiceClient
import zip.trev.trevrpc.benchmark.v1.BenchmarkSummary
import zip.trev.trevrpc.benchmark.v1.StreamRequest
import kotlin.time.Duration.Companion.seconds

public val BENCHMARK_OPERATION_TIMEOUT = 10.seconds

public data class BenchmarkWorkloadConfig(
    val rpcKind: BenchmarkRpcKind,
    val requestBytes: Int,
    val responseBytes: Int,
    val messagesPerStream: Int,
)

public data class MessageCounts(
    val requests: Long,
    val responses: Long,
)

public interface BenchmarkClient {
    public suspend fun unary(request: BenchmarkRequest): BenchmarkResponse

    public suspend fun clientStream(requests: Flow<List<BenchmarkRequest>>): BenchmarkSummary

    public fun serverStream(request: StreamRequest): Flow<BenchmarkResponse>

    public fun bidi(requests: Flow<List<BenchmarkRequest>>): Flow<BenchmarkResponse>
}

public class NativeBenchmarkClient(
    private val client: BenchmarkServiceClient,
) : BenchmarkClient {
    override suspend fun unary(request: BenchmarkRequest): BenchmarkResponse = client.unary(request)

    override suspend fun clientStream(requests: Flow<List<BenchmarkRequest>>): BenchmarkSummary {
        val call = client.clientStreamResponse()
        try {
            requests.collect(call::sendBatch)
            call.closeSend()
            return call.receive().message
        } finally {
            call.close()
        }
    }

    override fun serverStream(request: StreamRequest): Flow<BenchmarkResponse> =
        flow {
            val call = client.serverStreamResponse(request)
            try {
                while (true) {
                    val responses = call.receiveBatch()
                    if (responses.isEmpty()) break
                    responses.forEach { emit(it) }
                }
            } finally {
                call.close()
            }
        }

    override fun bidi(requests: Flow<List<BenchmarkRequest>>): Flow<BenchmarkResponse> =
        flow {
            val call = client.bidiResponse()
            try {
                coroutineScope {
                    val sender =
                        launch {
                            requests.collect(call::sendBatch)
                            call.closeSend()
                        }
                    try {
                        while (true) emit(call.receive() ?: break)
                        sender.join()
                    } catch (error: Throwable) {
                        sender.cancelAndJoin()
                        throw error
                    }
                }
            } finally {
                call.close()
            }
        }
}

public class BenchmarkWorkload(
    private val client: BenchmarkClient,
    private val config: BenchmarkWorkloadConfig,
) {
    private val requestPayload = ByteString.copyFrom(ByteArray(config.requestBytes))

    public suspend fun runOperation(): MessageCounts =
        kotlinx.coroutines.withTimeout(BENCHMARK_OPERATION_TIMEOUT) {
            when (config.rpcKind) {
                BenchmarkRpcKind.UNARY -> unary()
                BenchmarkRpcKind.CLIENT_STREAM -> clientStream()
                BenchmarkRpcKind.SERVER_STREAM -> serverStream()
                BenchmarkRpcKind.BIDI -> bidi()
            }
        }

    private suspend fun unary(): MessageCounts {
        val response = client.unary(request(0))
        validateResponse(response, 0)
        return counts()
    }

    private suspend fun clientStream(): MessageCounts {
        val summary = client.clientStream(requestBatches())
        check(summary.messageCount == config.messagesPerStream.toLong()) {
            "client stream returned ${summary.messageCount} messages, expected ${config.messagesPerStream}"
        }
        val expectedPayloadBytes = Math.multiplyExact(config.requestBytes.toLong(), config.messagesPerStream.toLong())
        check(summary.payloadBytes == expectedPayloadBytes) {
            "client stream returned ${summary.payloadBytes} payload bytes, expected $expectedPayloadBytes"
        }
        return counts()
    }

    private suspend fun serverStream(): MessageCounts {
        val request =
            StreamRequest
                .newBuilder()
                .setMessageCount(config.messagesPerStream)
                .setPayload(requestPayload)
                .setResponseBytes(config.responseBytes)
                .build()
        var sequence = 0L
        client.serverStream(request).collect { response ->
            validateResponse(response, sequence)
            sequence++
        }
        check(sequence == config.messagesPerStream.toLong()) {
            "server stream returned $sequence messages, expected ${config.messagesPerStream}"
        }
        return counts()
    }

    private suspend fun bidi(): MessageCounts {
        var sequence = 0L
        client.bidi(requestBatches()).collect { response ->
            validateResponse(response, sequence)
            sequence++
        }
        check(sequence == config.messagesPerStream.toLong()) {
            "bidi returned $sequence messages, expected ${config.messagesPerStream}"
        }
        return counts()
    }

    private fun requestBatches(): Flow<List<BenchmarkRequest>> =
        flow {
            var sequence = 0
            while (sequence < config.messagesPerStream) {
                val count = minOf(REQUEST_BATCH_SIZE, config.messagesPerStream - sequence)
                emit(List(count) { offset -> request((sequence + offset).toLong()) })
                sequence += count
            }
        }

    private fun request(sequence: Long): BenchmarkRequest =
        BenchmarkRequest
            .newBuilder()
            .setSequence(sequence)
            .setPayload(requestPayload)
            .setResponseBytes(config.responseBytes)
            .build()

    private fun validateResponse(
        response: BenchmarkResponse,
        expectedSequence: Long,
    ) {
        check(response.sequence == expectedSequence) {
            "response sequence ${response.sequence}, expected $expectedSequence"
        }
        check(response.payload.size() == config.responseBytes) {
            "response payload is ${response.payload.size()} bytes, expected ${config.responseBytes}"
        }
        val bytes = response.payload.asReadOnlyByteBuffer()
        while (bytes.hasRemaining()) check(bytes.get().toInt() == 0) { "response payload contents are invalid" }
    }

    private fun counts(): MessageCounts =
        MessageCounts(
            config.rpcKind.requestMessages(config.messagesPerStream),
            config.rpcKind.responseMessages(config.messagesPerStream),
        )
}

private const val REQUEST_BATCH_SIZE = 16
