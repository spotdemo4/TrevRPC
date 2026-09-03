package zip.trev.trevrpc.benchmark.support

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import zip.trev.trevrpc.benchmark.v1.BenchmarkRequest
import zip.trev.trevrpc.benchmark.v1.BenchmarkResponse
import zip.trev.trevrpc.benchmark.v1.BenchmarkSummary
import zip.trev.trevrpc.benchmark.v1.StreamRequest

class BenchmarkWorkloadTest {
    @Test
    fun `all four RPC shapes validate messages and counts`() =
        runBlocking {
            for (kind in BenchmarkRpcKind.entries) {
                val counts =
                    BenchmarkWorkload(
                        FakeBenchmarkClient(),
                        BenchmarkWorkloadConfig(kind, requestBytes = 17, responseBytes = 23, messagesPerStream = 5),
                    ).runOperation()
                assertEquals(kind.requestMessages(5), counts.requests, kind.wireName)
                assertEquals(kind.responseMessages(5), counts.responses, kind.wireName)
            }
        }

    private class FakeBenchmarkClient : BenchmarkClient {
        override suspend fun unary(request: BenchmarkRequest): BenchmarkResponse = response(request.sequence, request.responseBytes)

        override suspend fun clientStream(requests: Flow<List<BenchmarkRequest>>): BenchmarkSummary {
            var messageCount = 0L
            var payloadBytes = 0L
            requests.collect { batch ->
                batch.forEach { request ->
                    messageCount++
                    payloadBytes += request.payload.size()
                }
            }
            return BenchmarkSummary
                .newBuilder()
                .setMessageCount(messageCount)
                .setPayloadBytes(payloadBytes)
                .build()
        }

        override fun serverStream(request: StreamRequest): Flow<BenchmarkResponse> =
            flow {
                repeat(request.messageCount) { sequence ->
                    emit(response(sequence.toLong(), request.responseBytes))
                }
            }

        override fun bidi(requests: Flow<List<BenchmarkRequest>>): Flow<BenchmarkResponse> =
            flow {
                requests.collect { batch ->
                    batch.forEach { request -> emit(response(request.sequence, request.responseBytes)) }
                }
            }

        private fun response(
            sequence: Long,
            responseBytes: Int,
        ): BenchmarkResponse =
            BenchmarkResponse
                .newBuilder()
                .setSequence(sequence)
                .setPayload(ByteString.copyFrom(ByteArray(responseBytes)))
                .build()
    }
}
