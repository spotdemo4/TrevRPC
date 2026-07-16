package zip.trev.trevrpc.bench

import java.io.PrintStream
import java.nio.file.Path
import java.util.TreeMap

internal val PEER_NAME = System.getenv("TREVRPC_BENCH_PEER_NAME") ?: "kotlin"
internal const val MAX_APPLICATION_PAYLOAD_BYTES = 64 * 1024 * 1024
internal const val MAX_ENCODED_MESSAGE_BYTES = MAX_APPLICATION_PAYLOAD_BYTES + 1024
internal const val MAX_BENCHMARK_CONCURRENCY = 1024
internal const val MAX_MESSAGES_PER_STREAM = 1_000_000

internal sealed interface PeerCommand {
    data object Capabilities : PeerCommand

    data class Server(
        val stack: BenchmarkStack,
        val listen: String,
        val certificate: Path,
        val privateKey: Path,
        val webTransportOrigin: String? = null,
    ) : PeerCommand

    data class Client(
        val stack: BenchmarkStack,
        val address: String,
        val certificate: Path,
        val rpcKind: BenchmarkRpcKind,
        val concurrency: Int,
        val warmupMilliseconds: Long,
        val measurementMilliseconds: Long,
        val requestBytes: Int,
        val responseBytes: Int,
        val messagesPerStream: Int,
    ) : PeerCommand {
        val admissionNanoseconds: Long = Math.multiplyExact(measurementMilliseconds, 1_000_000L)
    }
}

internal enum class BenchmarkStack(
    val wireName: String,
) {
    TREVRPC_NATIVE_QUIC("trevrpc_native_quic"),
    GRPC_HTTP2("grpc_http2"),
    TREVRPC_WEBTRANSPORT("trevrpc_webtransport"),
    ;

    companion object {
        fun parse(value: String): BenchmarkStack =
            entries.firstOrNull { it.wireName == value }
                ?: throw IllegalArgumentException(
                    "--stack must be trevrpc_native_quic, grpc_http2, or trevrpc_webtransport",
                )
    }
}

internal enum class BenchmarkRpcKind(
    val wireName: String,
    val requestMessagesPerOperation: Long,
    val responseMessagesPerOperation: Long,
) {
    UNARY("unary", 1, 1),
    CLIENT_STREAM("client_stream", -1, 1),
    SERVER_STREAM("server_stream", 1, -1),
    BIDI("bidi", -1, -1),
    ;

    fun requestMessages(messagesPerStream: Int): Long =
        if (requestMessagesPerOperation < 0) messagesPerStream.toLong() else requestMessagesPerOperation

    fun responseMessages(messagesPerStream: Int): Long =
        if (responseMessagesPerOperation < 0) messagesPerStream.toLong() else responseMessagesPerOperation

    companion object {
        fun parse(value: String): BenchmarkRpcKind =
            entries.firstOrNull { it.wireName == value }
                ?: throw IllegalArgumentException("--rpc must be unary, client_stream, server_stream, or bidi")
    }
}

internal fun parseCommand(args: Array<String>): PeerCommand {
    require(args.isNotEmpty()) { "command is required" }
    return when (val command = args.first()) {
        "capabilities" -> {
            require(args.size == 1) { "capabilities does not accept options" }
            PeerCommand.Capabilities
        }

        "server" -> {
            parseServer(parseOptions(args.drop(1)))
        }

        "client" -> {
            parseClient(parseOptions(args.drop(1)))
        }

        else -> {
            throw IllegalArgumentException("unsupported command ${command.quoted()}")
        }
    }
}

private fun parseServer(values: Map<String, String>): PeerCommand.Server {
    requireKnown(values, setOf("stack", "listen", "cert", "key", "webtransport-origin"))
    val stack = BenchmarkStack.parse(required(values, "stack"))
    val webTransportOrigin = values["webtransport-origin"]?.takeIf(String::isNotEmpty)
    if (stack == BenchmarkStack.TREVRPC_WEBTRANSPORT) {
        require(webTransportOrigin != null) { "--webtransport-origin is required for trevrpc_webtransport servers" }
    } else {
        require(webTransportOrigin == null) { "--webtransport-origin is only valid for trevrpc_webtransport servers" }
    }
    return PeerCommand.Server(
        stack = stack,
        listen = required(values, "listen"),
        certificate = Path.of(required(values, "cert")),
        privateKey = Path.of(required(values, "key")),
        webTransportOrigin = webTransportOrigin,
    )
}

private fun parseClient(values: Map<String, String>): PeerCommand.Client {
    requireKnown(
        values,
        setOf(
            "address",
            "stack",
            "cert",
            "rpc",
            "concurrency",
            "warmup-ms",
            "measurement-ms",
            "request-bytes",
            "response-bytes",
            "messages-per-stream",
        ),
    )
    val stack = BenchmarkStack.parse(required(values, "stack"))
    require(stack != BenchmarkStack.TREVRPC_WEBTRANSPORT) {
        "trevrpc_webtransport is not supported by the Kotlin benchmark client"
    }
    val concurrency = positiveInt(values, "concurrency")
    require(concurrency <= MAX_BENCHMARK_CONCURRENCY) {
        "--concurrency must not exceed $MAX_BENCHMARK_CONCURRENCY"
    }
    val warmupMilliseconds = nonNegativeLong(values, "warmup-ms")
    require(warmupMilliseconds <= Long.MAX_VALUE / 1_000_000L) { "--warmup-ms is too large" }
    val measurementMilliseconds = positiveLong(values, "measurement-ms")
    require(measurementMilliseconds <= Long.MAX_VALUE / 1_000_000L) { "--measurement-ms is too large" }
    val requestBytes = nonNegativeInt(values, "request-bytes")
    val responseBytes = nonNegativeInt(values, "response-bytes")
    require(requestBytes <= MAX_APPLICATION_PAYLOAD_BYTES) {
        "--request-bytes must not exceed $MAX_APPLICATION_PAYLOAD_BYTES"
    }
    require(responseBytes <= MAX_APPLICATION_PAYLOAD_BYTES) {
        "--response-bytes must not exceed $MAX_APPLICATION_PAYLOAD_BYTES"
    }
    val messagesPerStream = positiveInt(values, "messages-per-stream")
    require(messagesPerStream <= MAX_MESSAGES_PER_STREAM) {
        "--messages-per-stream must not exceed $MAX_MESSAGES_PER_STREAM"
    }
    return PeerCommand.Client(
        stack = stack,
        address = required(values, "address"),
        certificate = Path.of(required(values, "cert")),
        rpcKind = BenchmarkRpcKind.parse(required(values, "rpc")),
        concurrency = concurrency,
        warmupMilliseconds = warmupMilliseconds,
        measurementMilliseconds = measurementMilliseconds,
        requestBytes = requestBytes,
        responseBytes = responseBytes,
        messagesPerStream = messagesPerStream,
    )
}

private fun parseOptions(args: List<String>): Map<String, String> {
    require(args.size % 2 == 0) { "every option requires a value" }
    val values = linkedMapOf<String, String>()
    for (index in args.indices step 2) {
        val option = args[index]
        require(option.startsWith("--") && option.length > 2) { "invalid option ${option.quoted()}" }
        val name = option.removePrefix("--")
        require(values.put(name, args[index + 1]) == null) { "duplicate option --$name" }
    }
    return values
}

private fun requireKnown(
    values: Map<String, String>,
    known: Set<String>,
) {
    val unknown = values.keys - known
    require(unknown.isEmpty()) { "unknown option --${unknown.first()}" }
}

private fun required(
    values: Map<String, String>,
    name: String,
): String = values[name]?.takeIf(String::isNotEmpty) ?: throw IllegalArgumentException("--$name is required")

private fun positiveInt(
    values: Map<String, String>,
    name: String,
): Int =
    required(values, name).toIntOrNull()?.also { require(it > 0) { "--$name must be positive" } }
        ?: throw IllegalArgumentException("--$name must be an integer")

private fun nonNegativeInt(
    values: Map<String, String>,
    name: String,
): Int =
    required(values, name).toIntOrNull()?.also { require(it >= 0) { "--$name must be non-negative" } }
        ?: throw IllegalArgumentException("--$name must be an integer")

private fun positiveLong(
    values: Map<String, String>,
    name: String,
): Long =
    required(values, name).toLongOrNull()?.also { require(it > 0) { "--$name must be positive" } }
        ?: throw IllegalArgumentException("--$name must be an integer")

private fun nonNegativeLong(
    values: Map<String, String>,
    name: String,
): Long =
    required(values, name).toLongOrNull()?.also { require(it >= 0) { "--$name must be non-negative" } }
        ?: throw IllegalArgumentException("--$name must be an integer")

internal data class HistogramBucket(
    val upperBoundNanoseconds: Long,
    val count: Long,
)

internal class LogLinearHistogram {
    private val counts = TreeMap<Long, Long>()

    val count: Long
        get() = counts.values.fold(0L, Math::addExact)

    fun record(valueNanoseconds: Long) {
        require(valueNanoseconds > 0) { "histogram values must be positive" }
        val upperBound = upperBound(valueNanoseconds)
        counts.merge(upperBound, 1L, Math::addExact)
    }

    fun add(other: LogLinearHistogram) {
        other.counts.forEach { (upperBound, count) -> counts.merge(upperBound, count, Math::addExact) }
    }

    fun buckets(): List<HistogramBucket> = counts.map { HistogramBucket(it.key, it.value) }

    companion object {
        fun upperBound(valueNanoseconds: Long): Long {
            require(valueNanoseconds > 0) { "histogram values must be positive" }
            val floorLog2 = Long.SIZE_BITS - 1 - java.lang.Long.numberOfLeadingZeros(valueNanoseconds)
            val shift = (floorLog2 - 9).coerceAtLeast(0)
            val next = (valueNanoseconds ushr shift) + 1
            if (next > (Long.MAX_VALUE ushr shift)) return Long.MAX_VALUE
            return (next shl shift) - 1
        }
    }
}

internal data class SampleResult(
    val rpcKind: BenchmarkRpcKind,
    val admissionNanoseconds: Long,
    val elapsedNanoseconds: Long,
    val completed: Long,
    val failed: Long,
    val requestMessages: Long,
    val responseMessages: Long,
    val histogram: LogLinearHistogram,
) {
    val drainNanoseconds: Long = (elapsedNanoseconds - admissionNanoseconds).coerceAtLeast(0)

    init {
        require(histogram.count == completed) { "histogram count must equal completed operations" }
    }
}

internal class EventWriter(
    private val output: PrintStream,
) {
    fun capabilities() {
        val rpcKinds = """["unary","client_stream","server_stream","bidi"]"""
        emit(
            """{"schema_version":4,"event":"capabilities","peer":${PEER_NAME.jsonString()},"roles":{""" +
                """"client":["trevrpc_native_quic","grpc_http2"],""" +
                """"server":["trevrpc_native_quic","trevrpc_webtransport","grpc_http2"]},""" +
                """"rpc_kinds":$rpcKinds,"histogram":"log_linear_v1"}""",
        )
    }

    fun ready(
        address: String,
        pid: Long,
    ) {
        emit(base("ready") + ",\"address\":${address.jsonString()},\"pid\":$pid}")
    }

    fun armed(pid: Long) {
        emit(base("armed") + ",\"pid\":$pid}")
    }

    fun sample(result: SampleResult) {
        val histogram =
            result.histogram.buckets().joinToString(prefix = "[", postfix = "]") { bucket ->
                """{"upper_bound_ns":"${bucket.upperBoundNanoseconds}","count":"${bucket.count}"}"""
            }
        emit(
            base("sample") +
                ",\"rpc_kind\":${result.rpcKind.wireName.jsonString()}" +
                ",\"admission_ns\":\"${result.admissionNanoseconds}\"" +
                ",\"elapsed_ns\":\"${result.elapsedNanoseconds}\"" +
                ",\"drain_ns\":\"${result.drainNanoseconds}\"" +
                ",\"completed\":\"${result.completed}\"" +
                ",\"failed\":\"${result.failed}\"" +
                ",\"request_messages\":\"${result.requestMessages}\"" +
                ",\"response_messages\":\"${result.responseMessages}\"" +
                ",\"histogram\":$histogram}",
        )
    }

    fun stopped() {
        emit(base("stopped") + "}")
    }

    fun error(
        phase: String,
        code: String,
        message: String,
    ) {
        emit(
            base("error") +
                ",\"phase\":${phase.jsonString()},\"code\":${code.jsonString()},\"message\":${message.jsonString()}}",
        )
    }

    @Synchronized
    private fun emit(json: String) {
        output.println(json)
        output.flush()
        check(!output.checkError()) { "failed to write benchmark protocol event" }
    }

    private fun base(event: String): String = """{"schema_version":4,"event":${event.jsonString()},"peer":${PEER_NAME.jsonString()}"""
}

private fun String.jsonString(): String =
    buildString(length + 2) {
        append('"')
        for (character in this@jsonString) {
            when (character) {
                '"' -> {
                    append("\\\"")
                }

                '\\' -> {
                    append("\\\\")
                }

                '\b' -> {
                    append("\\b")
                }

                '\u000c' -> {
                    append("\\f")
                }

                '\n' -> {
                    append("\\n")
                }

                '\r' -> {
                    append("\\r")
                }

                '\t' -> {
                    append("\\t")
                }

                else -> {
                    if (character.code < 0x20) {
                        append("\\u")
                        append(character.code.toString(16).padStart(4, '0'))
                    } else {
                        append(character)
                    }
                }
            }
        }
        append('"')
    }

private fun String.quoted(): String = "\"$this\""
