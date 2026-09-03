package zip.trev.trevrpc.bench.cronet

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.chromium.net.CronetEngine
import org.junit.Test
import org.junit.runner.RunWith
import zip.trev.trevrpc.CallOptions
import zip.trev.trevrpc.benchmark.support.BenchmarkRpcKind
import zip.trev.trevrpc.benchmark.support.BenchmarkWorkload
import zip.trev.trevrpc.benchmark.support.BenchmarkWorkloadConfig
import zip.trev.trevrpc.benchmark.support.NativeBenchmarkClient
import zip.trev.trevrpc.benchmark.v1.BenchmarkServiceClient
import zip.trev.trevrpc.cronet.CronetRpcChannel
import java.net.URI
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.seconds

@RunWith(AndroidJUnit4::class)
class CronetInteropSmokeTest {
    @Test(timeout = 120_000)
    fun allRpcShapes() {
        val arguments = InstrumentationRegistry.getArguments()
        val serverId = requireNotNull(arguments.getString("serverId")) { "serverId is required" }
        val origin = URI(requireNotNull(arguments.getString("origin")) { "origin is required" })
        require(origin.scheme == "https" && origin.host != null && origin.port in 1..65535) {
            "origin must be an https origin with an explicit port"
        }

        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val callbackExecutor = Executors.newFixedThreadPool(2)
        val engine =
            CronetEngine
                .Builder(context)
                .enableQuic(true)
                .addQuicHint(origin.host, origin.port, origin.port)
                .build()
        val channel = CronetRpcChannel.create(engine, origin.toString(), callbackExecutor)
        var failure: Throwable? = null
        try {
            runBlocking {
                withTimeout(60.seconds) {
                    val client =
                        NativeBenchmarkClient(
                            BenchmarkServiceClient(
                                channel,
                                CallOptions(
                                    timeout = 10.seconds,
                                    maxResponseBodySize = 1024,
                                    maxResponseMessages = 4,
                                    maxResponseStreamBodySize = null,
                                ),
                            ),
                        )
                    BenchmarkRpcKind.entries.forEach { rpcKind ->
                        try {
                            BenchmarkWorkload(
                                client,
                                BenchmarkWorkloadConfig(
                                    rpcKind = rpcKind,
                                    requestBytes = 16,
                                    responseBytes = 16,
                                    messagesPerStream = 4,
                                ),
                            ).runOperation()
                        } catch (error: Throwable) {
                            throw AssertionError("$serverId ${rpcKind.wireName} smoke failed", error)
                        }
                    }
                }
            }
        } catch (error: Throwable) {
            failure = error
            throw error
        } finally {
            var cleanupFailure: Throwable? = null

            fun cleanup(action: () -> Unit) {
                try {
                    action()
                } catch (error: Throwable) {
                    if (cleanupFailure == null) {
                        cleanupFailure = error
                    } else {
                        cleanupFailure.addSuppressed(error)
                    }
                }
            }

            cleanup { runBlocking { withTimeout(10.seconds) { channel.close() } } }
            cleanup(engine::shutdown)
            callbackExecutor.shutdown()
            cleanup {
                if (!callbackExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    callbackExecutor.shutdownNow()
                    error("Cronet callback executor did not terminate")
                }
            }
            cleanupFailure?.let { error ->
                if (failure == null) throw error
                failure.addSuppressed(error)
            }
        }
    }
}
