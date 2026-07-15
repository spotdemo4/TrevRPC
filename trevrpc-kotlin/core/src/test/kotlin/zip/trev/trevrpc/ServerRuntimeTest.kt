package zip.trev.trevrpc

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds

class ServerRuntimeTest {
    @Test
    fun `protocol validation and authorization run before route lookup`() =
        runTest {
            val authorizations = AtomicInteger()
            val server =
                Server(
                    authorizer =
                        Authorizer { request ->
                            authorizations.incrementAndGet()
                            if (!request.metadata.contains("allowed")) {
                                throw TrevRpcException(Status.unauthenticated("denied"))
                            }
                        },
                )

            val invalid = server.handleUnary(RpcRequest("secret.Service", "Hidden", version = 99u))
            assertEquals(Code.FAILED_PRECONDITION, invalid.status.code)
            assertEquals(0, authorizations.get())

            val denied = server.handleUnary(RpcRequest("secret.Service", "Hidden"))
            assertEquals(Code.UNAUTHENTICATED, denied.status.code)
            assertEquals(1, authorizations.get())

            val unknown =
                server.handleUnary(
                    RpcRequest("secret.Service", "Hidden", metadata = Metadata.of("allowed" to byteArrayOf())),
                )
            assertEquals(Code.UNIMPLEMENTED, unknown.status.code)
            assertEquals(2, authorizations.get())
            server.shutdown()
        }

    @Test
    fun `global admission rejects overload without invoking another handler`() =
        runTest {
            val entered = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val calls = AtomicInteger()
            val server = Server(ServerOptions(maxConcurrentRequests = 1))
            server.routeUnary("svc", "method") { _, _ ->
                calls.incrementAndGet()
                entered.complete(Unit)
                release.await()
                ResponseEnvelope(byteArrayOf(1))
            }

            val first = async { server.handleUnary(RpcRequest("svc", "method")) }
            entered.await()
            val rejected = server.handleUnary(RpcRequest("svc", "method"))
            assertEquals(Code.RESOURCE_EXHAUSTED, rejected.status.code)
            assertEquals(1, calls.get())
            release.complete(Unit)
            assertEquals(Code.OK, first.await().status.code)
            server.shutdown()
        }

    @Test
    fun `handler failures deadlines and stream idle timeout map to statuses`() =
        runTest {
            val server = Server(ServerOptions(streamIdleTimeout = 1.seconds))
            server.routeUnary("svc", "panic") { _, _ -> error("boom") }
            server.routeUnary("svc", "deadline") { _, _ -> awaitCancellation() }
            server.routeClientStreaming("svc", "idle") { _, requests ->
                requests.collect()
                ResponseEnvelope(byteArrayOf())
            }

            assertEquals(
                Code.INTERNAL,
                server.handleUnary(RpcRequest("svc", "panic")).status.code,
            )
            assertEquals(
                Code.DEADLINE_EXCEEDED,
                server.handleUnary(RpcRequest("svc", "deadline", timeoutNanos = 1_000_000_000u)).status.code,
            )
            val idle =
                server.handleStreaming(
                    RpcRequest("svc", "idle", kindValue = RpcKind.CLIENT_STREAMING.value),
                    flow { awaitCancellation() },
                )
            assertEquals(Code.UNAVAILABLE, checkNotNull(idle.receive()).status.code)
            server.shutdown()
        }

    @Test
    fun `server enforces request and response stream limits`() =
        runTest {
            val requestLimited = Server(ServerOptions(maxStreamMessages = 1))
            requestLimited.routeClientStreaming("svc", "upload") { _, requests ->
                requests.collect()
                ResponseEnvelope("done".encodeToByteArray())
            }
            val requestStream =
                requestLimited.handleStreaming(
                    RpcRequest("svc", "upload", kindValue = RpcKind.CLIENT_STREAMING.value),
                    flowOf(byteArrayOf(1), byteArrayOf(2)),
                )
            assertEquals(Code.RESOURCE_EXHAUSTED, checkNotNull(requestStream.receive()).status.code)
            requestLimited.shutdown()

            val responseLimited = Server(ServerOptions(maxStreamBodySize = 2))
            responseLimited.routeServerStreaming("svc", "download") { _, _ ->
                ResponseEnvelope(flowOf(byteArrayOf(1, 2), byteArrayOf(3)))
            }
            val responseStream =
                responseLimited.handleStreaming(
                    RpcRequest("svc", "download", kindValue = RpcKind.SERVER_STREAMING.value),
                    flowOf(),
                )
            assertEquals(RpcStreamFrameKind.MESSAGE, responseStream.receive()?.kind)
            assertEquals(Code.RESOURCE_EXHAUSTED, checkNotNull(responseStream.receive()).status.code)
            responseLimited.shutdown()
        }

    @Test
    fun `metrics are exact once for success rejection failure and early close`() =
        runTest {
            val metrics = RecordingMetrics()
            val server = Server(metrics = metrics)
            server.routeUnary("svc", "ok") { _, _ -> ResponseEnvelope(byteArrayOf(1, 2)) }
            server.routeUnary("svc", "fail") { _, _ -> throw TrevRpcException(Status(Code.PERMISSION_DENIED)) }
            server.routeServerStreaming("svc", "stream") { _, _ ->
                ResponseEnvelope(
                    flow {
                        emit(byteArrayOf(1))
                        awaitCancellation()
                    },
                )
            }

            assertEquals(Code.OK, server.handleUnary(RpcRequest("svc", "ok")).status.code)
            assertEquals(Code.PERMISSION_DENIED, server.handleUnary(RpcRequest("svc", "fail")).status.code)
            assertEquals(Code.UNIMPLEMENTED, server.handleUnary(RpcRequest("svc", "missing")).status.code)
            val stream =
                server.handleStreaming(
                    RpcRequest("svc", "stream", kindValue = RpcKind.SERVER_STREAMING.value),
                    flowOf(),
                )
            assertEquals(RpcStreamFrameKind.MESSAGE, stream.receive()?.kind)
            stream.close()

            assertEquals(4, metrics.started.get())
            assertEquals(4, metrics.finished.size)
            assertEquals(
                listOf(Code.OK, Code.PERMISSION_DENIED, Code.UNIMPLEMENTED, Code.CANCELLED),
                metrics.finished,
            )
            server.shutdown()
        }

    @Test
    fun `direct response sink failure completes lifecycle and propagates`() =
        runTest {
            val metrics = RecordingMetrics()
            val server = Server(metrics = metrics)
            server.routeServerStreaming("svc", "stream") { _, _ -> ResponseEnvelope(flowOf(byteArrayOf(1))) }
            val failure = IllegalStateException("write failed")

            val error =
                runCatching {
                    server.handleStreaming(
                        RpcRequest("svc", "stream", kindValue = RpcKind.SERVER_STREAMING.value),
                        flowOf(),
                    ) { throw failure }
                }.exceptionOrNull()

            assertTrue(error === failure)
            assertEquals(1, metrics.started.get())
            assertEquals(listOf(Code.CANCELLED), metrics.finished)
            server.shutdown()
        }

    @Test
    fun `graceful shutdown is idempotent cancels after timeout and rejects new work`() =
        runTest {
            val entered = CompletableDeferred<Unit>()
            val server = Server(ServerOptions(gracefulShutdownTimeout = 1.seconds))
            server.routeUnary("svc", "blocked") { _, _ ->
                entered.complete(Unit)
                awaitCancellation()
            }
            val active = async { server.handleUnary(RpcRequest("svc", "blocked")) }
            entered.await()

            server.shutdown()
            assertTrue(active.isCancelled)
            server.shutdown()
            val rejected = server.handleUnary(RpcRequest("svc", "blocked"))
            assertEquals(Code.UNAVAILABLE, rejected.status.code)
        }

    @Test
    fun `request context exposes shape metadata and deadline`() =
        runTest {
            var observed: RequestContext? = null
            val server = Server()
            server.routeUnary("svc", "context") { context, _ ->
                observed = context
                ResponseEnvelope(byteArrayOf())
            }
            val response =
                server.handleUnary(
                    RpcRequest(
                        "svc",
                        "context",
                        metadata = Metadata.of("key" to byteArrayOf(1)),
                        timeoutNanos = 5_000_000_000u,
                    ),
                )
            assertEquals(Code.OK, response.status.code)
            assertEquals(RpcKind.UNARY, observed?.kind)
            assertEquals(1, observed?.metadata?.get("key")?.single())
            assertFalse(checkNotNull(observed).deadlineExpired)
            assertTrue(checkNotNull(observed).timeRemaining()?.isPositive() == true)
            server.shutdown()
        }
}

private class RecordingMetrics : Metrics {
    val started = AtomicInteger()
    val finished: MutableList<Code> = Collections.synchronizedList(mutableListOf())

    override fun rpcStarted(event: RpcStarted) {
        started.incrementAndGet()
    }

    override fun rpcFinished(event: RpcFinished) {
        finished += event.code
    }
}
