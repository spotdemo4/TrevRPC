package zip.trev.trevrpc.examples

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import zip.trev.trevrpc.CallOptions
import zip.trev.trevrpc.Code
import zip.trev.trevrpc.RpcClientStream
import zip.trev.trevrpc.RpcRequest
import zip.trev.trevrpc.RpcResponse
import zip.trev.trevrpc.RpcStreamFrame
import zip.trev.trevrpc.RpcTransport
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

        override suspend fun openStream(request: RpcRequest): RpcClientStream {
            val requests = Channel<ByteArray>(1)
            val responses = server.handleStreaming(request, requests.receiveAsFlow())
            return object : RpcClientStream {
                override suspend fun send(body: ByteArray) {
                    requests.send(body.copyOf())
                }

                override suspend fun finishSend() {
                    requests.close()
                }

                override suspend fun receive(): RpcStreamFrame? = responses.receive()

                override suspend fun close(cause: Throwable?) {
                    requests.cancel(CancellationException("local request stream closed", cause))
                    responses.close(cause)
                }
            }
        }
    }
}
