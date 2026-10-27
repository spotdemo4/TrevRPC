package zip.trev.trevrpc

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import java.util.concurrent.CopyOnWriteArrayList

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

            /** A server ends the response stream after its STATUS, half-closed or not. */
            private var ended = false

            override suspend fun receive(): ResponseFrame? {
                if (ended) return null
                return frames.receiveCatching().getOrNull()?.also { ended = it is ResponseFrame.Status }
            }

            override suspend fun close(cause: Throwable?) {
                frames.close()
            }
        }
    }
}

/**
 * A transport whose every stream records what the client sends and answers with frames scripted per
 * stream, for calls that open more than one, such as a replayed optimistic call. Opening the
 * stream at index [gatedFrom] or later waits for [gate].
 */
internal class MultiStreamTransport(
    override val compression: Compression?,
    override val streamCompressionBudget: StreamCompressionBudget? = null,
    private val gatedFrom: Int = Int.MAX_VALUE,
) : RpcTransport {
    val gate = CompletableDeferred<Unit>()
    val streams = CopyOnWriteArrayList<Stream>()

    /** Receives each stream as it opens. */
    val opened = Channel<Stream>(Channel.UNLIMITED)

    inner class Stream(
        val request: RpcRequest,
    ) : RpcClientStream {
        private val frames = Channel<ResponseFrame>(Channel.UNLIMITED)
        private val sentFrames = CopyOnWriteArrayList<Pair<Long, ByteArray>>()

        @Volatile
        var finished = false
            private set

        @Volatile
        var closed = false
            private set

        /** Set to make every send fail with it, as a write to a stream the server stopped reading does. */
        @Volatile
        var sendFailure: Throwable? = null

        val sent: List<Pair<Long, ByteArray>>
            get() = sentFrames.toList()

        /** Queues response frames; the stream ends after a STATUS, as a server's does. */
        fun respond(vararg response: ResponseFrame) =
            response.forEach {
                frames.trySend(it)
                if (it is ResponseFrame.Status) frames.close()
            }

        override suspend fun send(
            body: ByteArray,
            frameType: Long,
        ) {
            if (closed) throw TrevRpcException(Status.cancelled("stream is closed"))
            sendFailure?.let { throw it }
            sentFrames += frameType to body.copyOf()
        }

        override suspend fun finishSend() {
            finished = true
        }

        override suspend fun receive(): ResponseFrame? = frames.receiveCatching().getOrNull()

        override suspend fun close(cause: Throwable?) {
            closed = true
            frames.close()
        }
    }

    override suspend fun openStream(
        request: RpcRequest,
        firstMessage: ByteArray?,
        finishSend: Boolean,
    ): RpcClientStream {
        if (streams.size >= gatedFrom) gate.await()
        return Stream(request).also {
            streams += it
            opened.trySend(it)
        }
    }
}
