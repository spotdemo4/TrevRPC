package zip.trev.trevrpc

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class ConnectionMetadataTest {
    private val strings = MessageCodec(String::encodeToByteArray, ByteArray::decodeToString)

    private fun metadata(
        key: String,
        value: String,
    ) = Metadata.of(key to value.encodeToByteArray())

    /** A transport that records each REQUEST and answers every unary call with "ok". */
    private class RecordingTransport(
        private val source: ConnectionMetadataSource? = null,
    ) : RpcTransport {
        val requests = mutableListOf<RpcRequest>()

        override val connectionMetadata: ConnectionMetadata?
            get() = source?.current()

        override suspend fun openStream(
            request: RpcRequest,
            firstMessage: ByteArray?,
            finishSend: Boolean,
        ): RpcClientStream {
            requests += request
            val frames =
                ArrayDeque<ResponseFrame>(listOf(ResponseFrame.Message("ok".encodeToByteArray()), ResponseFrame.Status(Status.ok())))
            return object : RpcClientStream {
                override suspend fun send(
                    body: ByteArray,
                    frameType: Long,
                ) = Unit

                override suspend fun finishSend() = Unit

                override suspend fun receive(): ResponseFrame? = frames.removeFirstOrNull()

                override suspend fun close(cause: Throwable?) = Unit
            }
        }
    }

    private suspend fun Client.call(options: CallOptions = CallOptions()) = unary("/svc/m", "x", strings, strings, options)

    @Test
    fun `sets get increasing IDs and empty metadata removes the set`() {
        val source = ConnectionMetadataSource(metadata("authorization", "a"))
        assertEquals(ConnectionMetadata(1u, metadata("authorization", "a")), source.current())
        source.set(metadata("authorization", "b"))
        assertEquals(ConnectionMetadata(2u, metadata("authorization", "b")), source.current())
        source.set(Metadata.EMPTY)
        assertNull(source.current())
        source.set(metadata("authorization", "c"))
        assertEquals(ConnectionMetadata(3u, metadata("authorization", "c")), source.current())
        assertNull(ConnectionMetadataSource().current())
    }

    @Test
    fun `every call offers the whole set next to its own metadata`() =
        runBlocking {
            val source = ConnectionMetadataSource(metadata("authorization", "ok"))
            val transport = RecordingTransport(source)
            val client = Client(transport)
            repeat(3) {
                client.call(CallOptions(metadata = metadata("x-a", "$it")))
            }

            assertEquals(3, transport.requests.size)
            transport.requests.forEachIndexed { index, request ->
                assertEquals(ConnectionMetadata(1u, metadata("authorization", "ok")), request.connectionMetadata)
                // The call's own entries stay out of the set.
                assertEquals(metadata("x-a", "$index"), request.metadata)
            }
        }

    @Test
    fun `replacing the set changes the offer of later calls and removing it stops the offer`() =
        runBlocking {
            val source = ConnectionMetadataSource(metadata("authorization", "old"))
            val transport = RecordingTransport(source)
            val client = Client(transport)
            client.call()
            source.set(metadata("authorization", "new"))
            client.call()
            source.set(Metadata.EMPTY)
            client.call()

            assertEquals(
                listOf(
                    ConnectionMetadata(1u, metadata("authorization", "old")),
                    ConnectionMetadata(2u, metadata("authorization", "new")),
                    null,
                ),
                transport.requests.map { it.connectionMetadata },
            )
        }

    @Test
    fun `a transport without connection metadata sends none`() =
        runBlocking {
            val transport = RecordingTransport()
            Client(transport).call()
            assertNull(transport.requests.single().connectionMetadata)
        }

    @Test
    fun `the offer survives the wire with an ID that needs several bytes`() {
        val request = RpcRequest("/svc/m", connectionMetadata = ConnectionMetadata(300u, metadata("authorization", "ok")))
        assertEquals(request, WireCodec.decodeRequest(WireCodec.encode(request)))
        val answer = RpcAnswer(connectionMetadataId = UInt.MAX_VALUE)
        assertEquals(answer, WireCodec.decodeAnswer(WireCodec.encode(answer)))
    }
}
