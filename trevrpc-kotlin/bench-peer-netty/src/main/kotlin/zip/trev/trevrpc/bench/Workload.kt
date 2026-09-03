package zip.trev.trevrpc.bench

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import zip.trev.trevrpc.CallOptions
import zip.trev.trevrpc.benchmark.support.BENCHMARK_OPERATION_TIMEOUT
import zip.trev.trevrpc.benchmark.support.BenchmarkWorkload
import zip.trev.trevrpc.benchmark.support.BenchmarkWorkloadConfig

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
        } catch (error: kotlinx.coroutines.CancellationException) {
            throw error
        } catch (error: Throwable) {
            failed = Math.addExact(failed, 1L)
            failure = error
            break
        }
    }
    return LaneResult(completed, failed, requestMessages, responseMessages, histogram, failure)
}

internal fun workloadConfig(config: PeerCommand.Client): BenchmarkWorkloadConfig =
    BenchmarkWorkloadConfig(
        rpcKind = config.rpcKind,
        requestBytes = config.requestBytes,
        responseBytes = config.responseBytes,
        messagesPerStream = config.messagesPerStream,
    )
