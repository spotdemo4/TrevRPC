package zip.trev.trevrpc.examples

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import zip.trev.trevrpc.CallOptions
import zip.trev.trevrpc.Code
import zip.trev.trevrpc.RpcRequest
import zip.trev.trevrpc.RpcResponse
import zip.trev.trevrpc.RpcTransport
import zip.trev.trevrpc.RpcTransportStream
import zip.trev.trevrpc.Server
import zip.trev.trevrpc.TrevRpcException
import zip.trev.trevrpc.WireCodec
import zip.trev.trevrpc.examples.greeter.GreeterClient

class GreeterServiceTest {
    @Test
    fun `registered service implements all four RPC shapes and authorization`() =
        runBlocking {
            val server = createGreeterServer("test-token")
            try {
                val transport = LocalTransport(server)
                val client = GreeterClient(transport, authenticatedOptions("test-token"))
                assertEquals(
                    listOf("hello, unary", "hello, server", "goodbye, server", "left,right", "echo, one", "echo, two"),
                    runReadableGreeterClient(client),
                )

                val denied =
                    assertThrows(TrevRpcException::class.java) {
                        runBlocking { GreeterClient(transport, CallOptions()).sayHello(request("denied")) }
                    }
                assertEquals(Code.UNAUTHENTICATED, denied.status.code)
            } finally {
                server.shutdown()
            }
        }

    @Test
    fun `malformed initial protobuf is rejected by wire decoder`() {
        assertThrows(TrevRpcException::class.java) {
            WireCodec.decodeRequest(byteArrayOf(0xff.toByte(), 0xff.toByte()))
        }
    }

    private class LocalTransport(
        private val server: Server,
    ) : RpcTransport {
        override suspend fun unary(request: RpcRequest): RpcResponse = server.handleUnary(request)

        override suspend fun openStream(
            request: RpcRequest,
            requestBody: Flow<ByteArray>,
        ): RpcTransportStream = server.handleStreaming(request, requestBody)
    }
}
