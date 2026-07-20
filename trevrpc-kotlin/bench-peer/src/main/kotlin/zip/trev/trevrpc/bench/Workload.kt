package zip.trev.trevrpc.bench

import com.google.protobuf.ByteString
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import zip.trev.trevrpc.CallOptions
import zip.trev.trevrpc.benchmark.v1.BenchmarkRequest
import zip.trev.trevrpc.benchmark.v1.BenchmarkResponse
import zip.trev.trevrpc.benchmark.v1.BenchmarkServiceClient
import zip.trev.trevrpc.benchmark.v1.BenchmarkSummary
import zip.trev.trevrpc.benchmark.v1.StreamRequest
import kotlin.time.Duration.Companion.seconds

internal val BENCHMARK_OPERATION_TIMEOUT = 10.seconds

internal data class MessageCounts(
    val requests: Long,
    val responses: Long,
)

internal interface BenchmarkClient {
    suspend fun unary(request: BenchmarkRequest): BenchmarkResponse

    suspend fun clientStream(requests: Flow<List<BenchmarkRequest>>): BenchmarkSummary

    fun serverStream(request: StreamRequest): Flow<BenchmarkResponse>

    fun bidi(requests: Flow<List<BenchmarkRequest>>): Flow<BenchmarkResponse>
}

internal class NativeBenchmarkClient(
    private val client: BenchmarkServiceClient,
) : BenchmarkClient {
    override suspend fun unary(request: BenchmarkRequest): BenchmarkResponse = client.unary(request)

    override suspend fun clientStream(requests: Flow<List<BenchmarkRequest>>): BenchmarkSummary {
        val call = client.clientStreamCall()
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
            val call = client.serverStreamCall(request)
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
            val call = client.bidiCall()
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

internal class BenchmarkWorkload(
    private val client: BenchmarkClient,
    private val config: PeerCommand.Client,
) {
    private val requestPayload = ByteString.copyFrom(ByteArray(config.requestBytes))

    suspend fun runOperation(): MessageCounts =
        withTimeout(BENCHMARK_OPERATION_TIMEOUT) {
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

internal fun benchmarkCallOptions(config: PeerCommand.Client): CallOptions =
    CallOptions(
        timeout = BENCHMARK_OPERATION_TIMEOUT,
        maxResponseBodySize = MAX_ENCODED_MESSAGE_BYTES,
        maxResponseMessages = config.messagesPerStream.coerceAtLeast(1),
        maxResponseStreamBodySize = null,
    )

internal data class PhaseResult(
    val elapsedNanoseconds: Long,
    val completed: Long,
    val failed: Long,
    val requestMessages: Long,
    val responseMessages: Long,
    val histogram: LogLinearHistogram,
    val errors: List<Throwable>,
)

internal suspend fun runWarmup(
    workload: BenchmarkWorkload,
    concurrency: Int,
    durationNanoseconds: Long,
) {
    if (durationNanoseconds == 0L) return
    val result = runPhase(workload, concurrency, durationNanoseconds, record = false)
    if (result.failed != 0L) {
        val cause = result.errors.firstOrNull()
        throw IllegalStateException("warmup recorded ${result.failed} failed operation(s)", cause)
    }
}

internal suspend fun runMeasurement(
    workload: BenchmarkWorkload,
    config: PeerCommand.Client,
    armed: () -> Unit,
    awaitStart: suspend () -> Unit,
): PhaseResult =
    runPhase(
        workload,
        config.concurrency,
        config.admissionNanoseconds,
        record = true,
        beforeStart = {
            armed()
            awaitStart()
        },
    )

private data class AdmissionWindow(
    val startNanoseconds: Long,
    val durationNanoseconds: Long,
)

private data class LaneResult(
    val completed: Long,
    val failed: Long,
    val requestMessages: Long,
    val responseMessages: Long,
    val histogram: LogLinearHistogram,
    val error: Throwable?,
)

private suspend fun runPhase(
    workload: BenchmarkWorkload,
    concurrency: Int,
    durationNanoseconds: Long,
    record: Boolean,
    beforeStart: suspend () -> Unit = {},
): PhaseResult =
    coroutineScope {
        val ready = Channel<Unit>(concurrency)
        val gate = CompletableDeferred<AdmissionWindow>()
        val lanes =
            List(concurrency) {
                async(Dispatchers.Default) {
                    ready.send(Unit)
                    runLane(workload, gate.await(), record)
                }
            }
        repeat(concurrency) { ready.receive() }
        ready.close()
        beforeStart()
        val start = System.nanoTime()
        gate.complete(AdmissionWindow(start, durationNanoseconds))
        val results = lanes.awaitAll()
        val elapsed = (System.nanoTime() - start).coerceAtLeast(0)
        val histogram = LogLinearHistogram()
        var completed = 0L
        var failed = 0L
        var requestMessages = 0L
        var responseMessages = 0L
        results.forEach { result ->
            completed = Math.addExact(completed, result.completed)
            failed = Math.addExact(failed, result.failed)
            requestMessages = Math.addExact(requestMessages, result.requestMessages)
            responseMessages = Math.addExact(responseMessages, result.responseMessages)
            histogram.add(result.histogram)
        }
        PhaseResult(
            elapsed,
            completed,
            failed,
            requestMessages,
            responseMessages,
            histogram,
            results.mapNotNull(LaneResult::error),
        )
    }

private suspend fun runLane(
    workload: BenchmarkWorkload,
    window: AdmissionWindow,
    record: Boolean,
): LaneResult {
    var completed = 0L
    var failed = 0L
    var requestMessages = 0L
    var responseMessages = 0L
    val histogram = LogLinearHistogram()
    var failure: Throwable? = null
    while (true) {
        val operationStart = System.nanoTime()
        if (operationStart - window.startNanoseconds >= window.durationNanoseconds) break
        try {
            val counts = workload.runOperation()
            completed = Math.addExact(completed, 1L)
            requestMessages = Math.addExact(requestMessages, counts.requests)
            responseMessages = Math.addExact(responseMessages, counts.responses)
            if (record) histogram.record((System.nanoTime() - operationStart).coerceAtLeast(1))
        } catch (error: CancellationException) {
            throw error
        } catch (error: Throwable) {
            failed = Math.addExact(failed, 1L)
            failure = error
            break
        }
    }
    return LaneResult(completed, failed, requestMessages, responseMessages, histogram, failure)
}
