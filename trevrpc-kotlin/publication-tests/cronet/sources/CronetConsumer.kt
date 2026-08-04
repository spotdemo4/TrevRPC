package trevrpc.publication.cronet

import org.chromium.net.CronetEngine
import zip.trev.trevrpc.RpcChannel
import zip.trev.trevrpc.cronet.CronetRpcChannel
import java.util.concurrent.Executor

fun createCronetChannel(
    engine: CronetEngine,
    callbackExecutor: Executor,
): RpcChannel = CronetRpcChannel.create(engine, "https://example.com", callbackExecutor)
