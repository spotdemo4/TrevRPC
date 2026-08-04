package zip.trev.trevrpc.publication

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import zip.trev.trevrpc.BidirectionalStreamingCall
import zip.trev.trevrpc.ClientStreamingCall
import zip.trev.trevrpc.ResponseEnvelope
import zip.trev.trevrpc.RpcTransport
import zip.trev.trevrpc.ServerStreamingCall

fun client(transport: RpcTransport): AllShapesClient = AllShapesClient(transport)

suspend fun unary(
    client: AllShapesClient,
    request: Request,
): Response = client.unary(request)

suspend fun unaryResponse(
    client: AllShapesClient,
    request: Request,
): ResponseEnvelope<Response> = client.unaryResponse(request)

fun serverStreaming(
    client: AllShapesClient,
    request: Request,
): Flow<Response> = client.serverStreaming(request)

suspend fun serverStreamingResponse(
    client: AllShapesClient,
    request: Request,
): ServerStreamingCall<Response> = client.serverStreamingResponse(request)

suspend fun clientStreaming(
    client: AllShapesClient,
    request: Request,
): Response = client.clientStreaming(flowOf(request))

suspend fun clientStreamingResponse(
    client: AllShapesClient,
    request: Request,
): ResponseEnvelope<Response> = client.clientStreamingResponse(flowOf(request))

fun bidirectionalStreaming(
    client: AllShapesClient,
    request: Request,
): Flow<Response> = client.bidirectionalStreaming(flowOf(request))

suspend fun interactiveClientStreaming(client: AllShapesClient): ClientStreamingCall<Request, Response> = client.clientStreamingResponse()

suspend fun interactiveBidirectionalStreaming(client: AllShapesClient): BidirectionalStreamingCall<Request, Response> =
    client.bidirectionalStreamingResponse()
