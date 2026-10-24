package zip.trev.trevrpc

import kotlinx.coroutines.channels.Channel

/** The compression golden vectors, every value decoded from hex. */
internal fun zstdVectors(): Map<String, ByteArray> =
    checkNotNull(ScriptedTransport::class.java.classLoader.getResourceAsStream("zstd-golden-vectors.txt"))
        .bufferedReader()
        .useLines { lines ->
            lines
                .map(String::trim)
                .filter { it.isNotEmpty() && !it.startsWith('#') }
                .associate { line ->
                    val (name, value) = line.split('=', limit = 2).map(String::trim)
                    name to value.chunked(2).map { it.toInt(16).toByte() }.toByteArray()
                }
        }

/** A transport with one call at a time: it records what the client sends and answers with scripted frames. */
internal class ScriptedTransport(
    override val compression: Compression?,
    override val streamCompressionBudget: StreamCompressionBudget? = null,
) : RpcTransport {
    private val frames = Channel<ResponseFrame>(Channel.UNLIMITED)
    val requests = mutableListOf<RpcRequest>()
    val sent = mutableListOf<Pair<Long, ByteArray>>()
    val firstMessages = mutableListOf<ByteArray?>()

    fun respond(vararg response: ResponseFrame) = response.forEach { frames.trySend(it) }

    /** Ends the response stream after the frames sent so far. */
    fun end() = frames.close()

    override suspend fun openStream(
        request: RpcRequest,
        firstMessage: ByteArray?,
        finishSend: Boolean,
    ): RpcClientStream {
        requests += request
        firstMessages += firstMessage
        return object : RpcClientStream {
            override suspend fun send(
                body: ByteArray,
                frameType: Long,
            ) {
                sent += frameType to body.copyOf()
            }

            override suspend fun finishSend() {
                frames.close()
            }

            override suspend fun receive(): ResponseFrame? = frames.receiveCatching().getOrNull()

            override suspend fun close(cause: Throwable?) {
                frames.close()
            }
        }
    }
}
