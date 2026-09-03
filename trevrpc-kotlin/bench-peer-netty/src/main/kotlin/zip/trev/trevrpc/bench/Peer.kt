package zip.trev.trevrpc.bench

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import zip.trev.trevrpc.benchmark.support.BENCHMARK_OPERATION_TIMEOUT
import zip.trev.trevrpc.benchmark.support.BenchmarkClient
import zip.trev.trevrpc.benchmark.support.NativeBenchmarkClient
import zip.trev.trevrpc.benchmark.v1.BenchmarkServiceClient
import zip.trev.trevrpc.netty.NettyClientTls
import zip.trev.trevrpc.netty.NettyQuicClientConfig
import zip.trev.trevrpc.netty.NettyRpcServer
import zip.trev.trevrpc.netty.NettyRpcServerConfig
import zip.trev.trevrpc.netty.NettyServerTls
import zip.trev.trevrpc.netty.NettyTransportOptions
import zip.trev.trevrpc.netty.WEBTRANSPORT_PATH
import zip.trev.trevrpc.netty.WebTransportAdmission
import zip.trev.trevrpc.netty.advanced.RawNettyQuicRpcTransport
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import kotlin.system.exitProcess
import kotlin.time.Duration.Companion.minutes

fun main(args: Array<String>) {
    val events = EventWriter(System.out)
    try {
        when (val command = parseCommand(args)) {
            PeerCommand.Capabilities -> events.capabilities()
            is PeerCommand.Server -> runBlocking { runServer(command, events, controlReader()) }
            is PeerCommand.Client -> runBlocking { runClient(command, events, controlReader()) }
        }
    } catch (error: Throwable) {
        val summary = error.message ?: error.javaClass.name
        val cause = error.cause
        val message = cause?.let { "$summary: ${it.message ?: it.javaClass.name}" } ?: summary
        System.err.println(message)
        cause?.printStackTrace(System.err)
        runCatching { events.error(error.phase(), error.code(), message) }
        exitProcess(1)
    }
}

private suspend fun runServer(
    config: PeerCommand.Server,
    events: EventWriter,
    control: BufferedReader,
) {
    val server =
        try {
            bindBenchmarkServer(config)
        } catch (error: Throwable) {
            throw PeerFailure("serve", "bind_failed", "failed to bind benchmark server", error)
        }
    val shutdownHook =
        Thread(
            { runBlocking { runCatching { server.shutdown() } } },
            "trevrpc-bench-peer-kotlin-shutdown",
        )
    Runtime.getRuntime().addShutdownHook(shutdownHook)
    try {
        events.ready(formatAddress(server.localAddress), ProcessHandle.current().pid())
        when (val command = readControl(control)) {
            "SHUTDOWN" -> Unit

            null -> awaitCancellation()

            else -> throw PeerFailure(
                "control",
                "unexpected_command",
                "server expected SHUTDOWN, received ${command.quoted()}",
            )
        }
        server.shutdown()
        events.stopped()
    } finally {
        runCatching { Runtime.getRuntime().removeShutdownHook(shutdownHook) }
        runCatching { server.shutdown() }
    }
}

private suspend fun runClient(
    config: PeerCommand.Client,
    events: EventWriter,
    control: BufferedReader,
) {
    val connection =
        try {
            withTimeout(BENCHMARK_OPERATION_TIMEOUT) { connectBenchmarkClient(config) }
        } catch (error: Throwable) {
            throw PeerFailure("connect", "connection_failed", "failed to configure verified benchmark connection", error)
        }
    try {
        val workload = BenchmarkWorkload(connection.client, workloadConfig(config))
        try {
            workload.runOperation()
        } catch (error: Throwable) {
            throw PeerFailure("validate", "rpc_failed", "validation RPC failed", error)
        }
        try {
            val warmupNanoseconds = Math.multiplyExact(config.warmupMilliseconds, 1_000_000L)
            runWarmup(workload, config.concurrency, warmupNanoseconds)
        } catch (error: Throwable) {
            throw PeerFailure("warmup", "rpc_failed", "warmup failed", error)
        }
        val measured =
            runMeasurement(
                workload,
                config,
                armed = { events.armed(ProcessHandle.current().pid()) },
                awaitStart = {
                    val command = readControl(control)
                    if (command != "START") {
                        throw PeerFailure(
                            "control",
                            "unexpected_command",
                            "client expected START, received ${command?.quoted() ?: "end of input"}",
                        )
                    }
                },
            )
        measured.errors.forEach { error ->
            System.err.println("benchmark operation failed: ${error.message ?: error.javaClass.name}")
        }
        events.sample(
            SampleResult(
                config.rpcKind,
                config.admissionNanoseconds,
                measured.elapsedNanoseconds,
                measured.completed,
                measured.failed,
                measured.requestMessages,
                measured.responseMessages,
                measured.histogram,
            ),
        )
    } finally {
        connection.shutdown()
    }
}

private suspend fun bindBenchmarkServer(config: PeerCommand.Server): RunningBenchmarkServer {
    val address = parseAddress(config.listen, allowZeroPort = true)
    return when (config.stack) {
        BenchmarkStack.TREVRPC_NATIVE_QUIC -> {
            val server =
                NettyRpcServer.bind(
                    createBenchmarkServer(),
                    benchmarkNettyServerConfig(config, address),
                )
            object : RunningBenchmarkServer {
                override val localAddress = server.localAddress

                override suspend fun shutdown() = server.shutdown()
            }
        }

        BenchmarkStack.TREVRPC_WEBTRANSPORT -> {
            val server =
                NettyRpcServer.bind(
                    createBenchmarkServer(),
                    benchmarkNettyServerConfig(config, address),
                )
            object : RunningBenchmarkServer {
                override val localAddress = server.localAddress

                override suspend fun shutdown() = server.shutdown()
            }
        }
    }
}

private suspend fun connectBenchmarkClient(config: PeerCommand.Client): BenchmarkClientConnection {
    val address = parseAddress(config.address, allowZeroPort = false)
    return when (config.stack) {
        BenchmarkStack.TREVRPC_NATIVE_QUIC -> {
            val transport =
                RawNettyQuicRpcTransport.connect(
                    NettyQuicClientConfig(
                        remoteAddress = address,
                        tls =
                            NettyClientTls(
                                serverName = address.hostString,
                                trustCertificates = listOf(readCertificate(config.certificate)),
                                verifyHostname = true,
                            ),
                        options = benchmarkTransportOptions(),
                    ),
                )
            BenchmarkClientConnection(
                NativeBenchmarkClient(BenchmarkServiceClient(transport, benchmarkCallOptions(config))),
                transport::shutdown,
            )
        }

        BenchmarkStack.TREVRPC_WEBTRANSPORT -> {
            throw IllegalArgumentException("trevrpc_webtransport is not supported by the Kotlin benchmark client")
        }
    }
}

internal fun benchmarkNettyServerConfig(
    config: PeerCommand.Server,
    address: InetSocketAddress,
): NettyRpcServerConfig {
    val webTransport = config.stack == BenchmarkStack.TREVRPC_WEBTRANSPORT
    val expectedOrigin =
        if (webTransport) {
            requireNotNull(config.webTransportOrigin) {
                "trevrpc_webtransport server requires --webtransport-origin"
            }
        } else {
            null
        }
    return NettyRpcServerConfig(
        bindAddress = address,
        tls = NettyServerTls.Pem(config.privateKey.toFile(), config.certificate.toFile()),
        enableNative = !webTransport,
        enableHttp3 = webTransport,
        enableWebTransport = webTransport,
        webTransportAdmission =
            if (webTransport) {
                WebTransportAdmission { request ->
                    request.path == WEBTRANSPORT_PATH && request.secure && request.origin == expectedOrigin
                }
            } else {
                null
            },
        options = benchmarkTransportOptions(),
    )
}

private interface RunningBenchmarkServer {
    val localAddress: InetSocketAddress

    suspend fun shutdown()
}

private data class BenchmarkClientConnection(
    val client: BenchmarkClient,
    val shutdown: suspend () -> Unit,
)

private fun benchmarkTransportOptions(): NettyTransportOptions =
    NettyTransportOptions(
        maxFrameSize = MAX_ENCODED_MESSAGE_BYTES,
        workerParallelism = Runtime.getRuntime().availableProcessors().coerceAtLeast(2),
        inboundQueueCapacity = MAX_MESSAGES_PER_STREAM + 1,
        maxIdleTime = 10.minutes,
    )

internal fun parseAddress(
    value: String,
    allowZeroPort: Boolean,
): InetSocketAddress {
    val separator = value.lastIndexOf(':')
    require(separator > 0 && separator < value.lastIndex) { "invalid address ${value.quoted()}" }
    val rawHost = value.substring(0, separator)
    val host = rawHost.removePrefix("[").removeSuffix("]")
    require(host.isNotEmpty()) { "invalid address ${value.quoted()}" }
    val port = value.substring(separator + 1).toIntOrNull()
    val range = if (allowZeroPort) 0..65535 else 1..65535
    require(port != null && port in range) { "invalid address port in ${value.quoted()}" }
    return InetSocketAddress(host, port)
}

internal fun formatAddress(address: InetSocketAddress): String {
    val host = address.address.hostAddress
    return if (host.contains(':')) "[$host]:${address.port}" else "$host:${address.port}"
}

internal fun readCertificate(path: java.nio.file.Path): X509Certificate =
    Files.newInputStream(path).use { input ->
        CertificateFactory.getInstance("X.509").generateCertificate(input) as X509Certificate
    }

private fun controlReader(): BufferedReader = BufferedReader(InputStreamReader(System.`in`, StandardCharsets.US_ASCII))

private suspend fun readControl(reader: BufferedReader): String? = withContext(Dispatchers.IO) { reader.readLine() }

private class PeerFailure(
    val phase: String,
    val errorCode: String,
    message: String,
    cause: Throwable? = null,
) : RuntimeException(message, cause)

private fun Throwable.phase(): String = (this as? PeerFailure)?.phase ?: "config"

private fun Throwable.code(): String = (this as? PeerFailure)?.errorCode ?: "invalid_configuration"

private fun String.quoted(): String = "\"$this\""
