package zip.trev.trevrpc.benchmark.support

public enum class BenchmarkRpcKind(
    public val wireName: String,
    private val requestMessagesPerOperation: Long,
    private val responseMessagesPerOperation: Long,
) {
    UNARY("unary", 1, 1),
    CLIENT_STREAM("client_stream", -1, 1),
    SERVER_STREAM("server_stream", 1, -1),
    BIDI("bidi", -1, -1),
    ;

    public fun requestMessages(messagesPerStream: Int): Long =
        if (requestMessagesPerOperation < 0) messagesPerStream.toLong() else requestMessagesPerOperation

    public fun responseMessages(messagesPerStream: Int): Long =
        if (responseMessagesPerOperation < 0) messagesPerStream.toLong() else responseMessagesPerOperation

    public companion object {
        public fun parse(value: String): BenchmarkRpcKind =
            entries.firstOrNull { it.wireName == value }
                ?: throw IllegalArgumentException("--rpc must be unary, client_stream, server_stream, or bidi")
    }
}
