package zip.trev.trevrpc.examples

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import zip.trev.trevrpc.Authorizer
import zip.trev.trevrpc.Metadata
import zip.trev.trevrpc.RequestContext
import zip.trev.trevrpc.ResponseEnvelope
import zip.trev.trevrpc.Server
import zip.trev.trevrpc.ServerOptions
import zip.trev.trevrpc.Status
import zip.trev.trevrpc.TrevRpcException
import zip.trev.trevrpc.examples.greeter.GreeterClient
import zip.trev.trevrpc.examples.greeter.GreeterService
import zip.trev.trevrpc.examples.greeter.HelloReply
import zip.trev.trevrpc.examples.greeter.HelloRequest
import zip.trev.trevrpc.examples.greeter.registerGreeter
import java.security.MessageDigest
import kotlin.time.Duration.Companion.seconds

class ExampleGreeterService(
    private val responseMetadata: Metadata = Metadata.EMPTY,
) : GreeterService {
    override suspend fun sayHello(
        context: RequestContext,
        request: HelloRequest,
    ): ResponseEnvelope<HelloReply> = ResponseEnvelope(reply("hello, ${request.name}"), responseMetadata)

    override suspend fun lotsOfReplies(
        context: RequestContext,
        request: HelloRequest,
    ): ResponseEnvelope<Flow<HelloReply>> =
        ResponseEnvelope(
            flowOf(
                reply("hello, ${request.name}"),
                reply("goodbye, ${request.name}"),
            ),
            responseMetadata,
        )

    override suspend fun lotsOfGreetings(
        context: RequestContext,
        requests: Flow<HelloRequest>,
    ): ResponseEnvelope<HelloReply> = ResponseEnvelope(reply(requests.toList().joinToString(separator = ",") { it.name }), responseMetadata)

    override suspend fun bidiHello(
        context: RequestContext,
        requests: Flow<HelloRequest>,
    ): ResponseEnvelope<Flow<HelloReply>> = ResponseEnvelope(requests.map { request -> reply("echo, ${request.name}") }, responseMetadata)

    private fun reply(message: String): HelloReply = HelloReply.newBuilder().setMessage(message).build()
}

fun createGreeterServer(
    token: String,
    options: ServerOptions = ServerOptions(),
): Server {
    require(token.isNotEmpty()) { "bearer token must not be empty" }
    val expected = "Bearer $token".encodeToByteArray()
    val server =
        Server(
            options = options,
            authorizer =
                Authorizer { request ->
                    val supplied = request.metadata["authorization"]
                    if (supplied == null || !MessageDigest.isEqual(expected, supplied)) {
                        throw TrevRpcException(Status.unauthenticated("missing or invalid bearer token"))
                    }
                },
        )
    registerGreeter(server, ExampleGreeterService())
    return server
}

fun authenticatedOptions(token: String): zip.trev.trevrpc.CallOptions =
    zip.trev.trevrpc.CallOptions(
        timeout = 5.seconds,
        metadata = Metadata.of("authorization" to "Bearer $token".encodeToByteArray()),
    )

suspend fun runReadableGreeterClient(client: GreeterClient): List<String> {
    val results = mutableListOf<String>()
    results += client.sayHello(request("unary")).message
    client.lotsOfReplies(request("server")).collect { results += it.message }
    results += client.lotsOfGreetings(flowOf(request("left"), request("right"))).message
    client.bidiHello(flowOf(request("one"), request("two"))).collect { results += it.message }
    return results
}

internal fun request(name: String): HelloRequest = HelloRequest.newBuilder().setName(name).build()
