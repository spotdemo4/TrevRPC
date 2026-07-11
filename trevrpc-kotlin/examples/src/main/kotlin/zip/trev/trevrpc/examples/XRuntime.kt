package zip.trev.trevrpc.examples

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import zip.trev.trevrpc.CallOptions
import zip.trev.trevrpc.Code
import zip.trev.trevrpc.RpcTransport
import zip.trev.trevrpc.ServerOptions
import zip.trev.trevrpc.TrevRpcException
import zip.trev.trevrpc.examples.greeter.GreeterClient
import zip.trev.trevrpc.netty.Http3Admission
import zip.trev.trevrpc.netty.NettyClientTls
import zip.trev.trevrpc.netty.NettyHttp3RpcTransport
import zip.trev.trevrpc.netty.NettyQuicClientConfig
import zip.trev.trevrpc.netty.NettyQuicRpcTransport
import zip.trev.trevrpc.netty.NettyRpcServer
import zip.trev.trevrpc.netty.NettyRpcServerConfig
import zip.trev.trevrpc.netty.NettyServerTls
import zip.trev.trevrpc.netty.WebTransportAdmission
import java.net.InetSocketAddress
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.system.exitProcess
import kotlin.time.Duration.Companion.seconds

private const val DEFAULT_ADDR = "127.0.0.1:0"
private const val DEFAULT_TOKEN = "cross-runtime-token"
private const val DEFAULT_BROWSER_TOKEN = "trevrpc-example-token"

internal enum class Mode {
    SERVER,
    CLIENT,
    LIFECYCLE_CLIENT,
    BROWSER_SERVER,
}

internal enum class ClientTransport {
    NATIVE,
    HTTP3,
}

internal data class CliOptions(
    val mode: Mode,
    val address: String,
    val certificate: Path,
    val token: String,
    val iterations: Int,
    val transport: ClientTransport,
    val browserOrigins: Set<String>,
    val browserAuthorities: Set<String>,
    val maxConcurrentStreams: Int?,
)

fun main(args: Array<String>) {
    try {
        runBlocking { run(parseCli(args)) }
    } catch (error: Throwable) {
        System.err.println(error.message ?: error.javaClass.name)
        exitProcess(1)
    }
}

internal fun parseCli(
    args: Array<String>,
    environment: Map<String, String> = System.getenv(),
): CliOptions {
    val values = linkedMapOf<String, String>()
    var index = 0
    while (index < args.size) {
        val option = args[index]
        require(option.startsWith('-') && option.length > 1) { "unexpected argument $option" }
        require(index + 1 < args.size) { "$option requires a value" }
        require(values.put(option.removePrefix("--").removePrefix("-"), args[index + 1]) == null) {
            "duplicate option $option"
        }
        index += 2
    }
    val known =
        setOf(
            "mode",
            "addr",
            "cert",
            "token",
            "iterations",
            "transport",
            "origin",
            "authority",
            "browser-origin",
            "browser-authority",
            "max-streams",
        )
    val unknown = values.keys - known
    require(unknown.isEmpty()) { "unknown option -${unknown.first()}" }

    val mode =
        when (values["mode"] ?: "") {
            "server" -> Mode.SERVER
            "client" -> Mode.CLIENT
            "lifecycle-client" -> Mode.LIFECYCLE_CLIENT
            "browser-server" -> Mode.BROWSER_SERVER
            else -> throw IllegalArgumentException("unsupported -mode ${values["mode"].orEmpty().quoted()}")
        }
    val iterations = values["iterations"]?.toIntOrNull() ?: 1
    require(iterations > 0) { "-iterations must be positive" }
    val transport =
        when (values["transport"] ?: "native") {
            "native" -> ClientTransport.NATIVE
            "http3", "h3" -> ClientTransport.HTTP3
            else -> throw IllegalArgumentException("-transport must be native or http3")
        }
    val certificate =
        values["cert"]
            ?: environment["TREVRPC_EXAMPLE_CERT"]
            ?: throw IllegalArgumentException("-cert is required")
    val explicitOrigins = values["browser-origin"] ?: values["origin"]
    val explicitAuthorities = values["browser-authority"] ?: values["authority"]
    val maxConcurrentStreams =
        (values["max-streams"] ?: environment["TREVRPC_EXAMPLE_MAX_STREAMS"])?.let { raw ->
            raw.toIntOrNull()?.also { require(it > 0) { "maximum streams must be positive" } }
                ?: throw IllegalArgumentException("maximum streams must be an integer")
        }
    return CliOptions(
        mode = mode,
        address = values["addr"] ?: environment["TREVRPC_EXAMPLE_ADDR"] ?: DEFAULT_ADDR,
        certificate = Path.of(certificate),
        token =
            values["token"]
                ?: environment["TREVRPC_EXAMPLE_TOKEN"]
                ?: if (mode == Mode.BROWSER_SERVER) DEFAULT_BROWSER_TOKEN else DEFAULT_TOKEN,
        iterations = iterations,
        transport = transport,
        browserOrigins = commaSeparated(explicitOrigins ?: environment["TREVRPC_EXAMPLE_ORIGIN"]),
        browserAuthorities =
            commaSeparated(explicitAuthorities ?: environment["TREVRPC_EXAMPLE_AUTHORITIES"]),
        maxConcurrentStreams = maxConcurrentStreams,
    )
}

private suspend fun run(options: CliOptions) {
    when (options.mode) {
        Mode.SERVER, Mode.BROWSER_SERVER -> runServer(options)
        Mode.CLIENT -> repeat(options.iterations) { runClientIteration(options, it + 1) }
        Mode.LIFECYCLE_CLIENT -> repeat(options.iterations) { runLifecycleIteration(options, it + 1) }
    }
}

private suspend fun runServer(options: CliOptions) {
    val identity = generateLocalIdentity()
    writeCertificate(options.certificate, identity.certificate)
    val lifecycleShutdown = CompletableDeferred<Unit>()
    val serverOptions =
        options.maxConcurrentStreams?.let { ServerOptions(maxConcurrentStreamsPerConnection = it) }
            ?: ServerOptions()
    val core = createGreeterServer(options.token, serverOptions)
    if (options.mode == Mode.BROWSER_SERVER) registerBrowserLifecycle(core, lifecycleShutdown)
    val admission = LocalAdmission(options.browserOrigins, options.browserAuthorities)
    val netty =
        NettyRpcServer.bind(
            core,
            NettyRpcServerConfig(
                bindAddress = parseAddress(options.address),
                tls = NettyServerTls.KeyAndCertificates(identity.privateKey, listOf(identity.certificate)),
                enableNative = true,
                enableHttp3 = true,
                enableWebTransport = true,
                http3Admission = Http3Admission(admission::admitHttp3),
                webTransportAdmission = WebTransportAdmission(admission::admitWebTransport),
            ),
        )
    admission.bound(netty.localAddress)
    if (options.mode == Mode.BROWSER_SERVER) {
        println("READY https://${formatAddress(netty.localAddress)}/trevrpc")
    } else {
        println("READY ${formatAddress(netty.localAddress)}")
    }
    println("certificate written to ${options.certificate.toAbsolutePath()}")
    System.out.flush()

    val stopped = AtomicBoolean(false)
    val shutdownHook =
        Thread(
            {
                if (stopped.compareAndSet(false, true)) runBlocking { netty.shutdown() }
            },
            "trevrpc-xruntime-shutdown",
        )
    Runtime.getRuntime().addShutdownHook(shutdownHook)
    try {
        if (options.mode == Mode.BROWSER_SERVER) lifecycleShutdown.await() else awaitCancellation()
    } finally {
        runCatching { Runtime.getRuntime().removeShutdownHook(shutdownHook) }
        if (stopped.compareAndSet(false, true)) netty.shutdown()
    }
}

private suspend fun runClientIteration(
    options: CliOptions,
    iteration: Int,
) {
    val connection = connect(options)
    try {
        val unauthenticated = GreeterClient(connection.transport, CallOptions(timeout = 5.seconds))
        val authError = runCatching { unauthenticated.sayHello(request("unauthenticated")) }.exceptionOrNull()
        check(authError is TrevRpcException && authError.status.code == Code.UNAUTHENTICATED) {
            "client iteration $iteration: unauthenticated call returned $authError"
        }

        val client = GreeterClient(connection.transport, authenticatedOptions(options.token))
        val actual = runReadableGreeterClient(client)
        val expected = listOf("hello, unary", "hello, server", "goodbye, server", "left,right", "echo, one", "echo, two")
        check(actual == expected) { "client iteration $iteration: Greeter results $actual, expected $expected" }
    } finally {
        connection.shutdown()
    }
}

private suspend fun runLifecycleIteration(
    options: CliOptions,
    iteration: Int,
) {
    val connection = connect(options)
    try {
        val client = GreeterClient(connection.transport, authenticatedOptions(options.token))
        val unary = client.sayHello(request("lifecycle-unary"))
        check(unary.message == "hello, lifecycle-unary") { "lifecycle client iteration $iteration: unary failed" }

        val serverStream = client.lotsOfRepliesCall(request("lifecycle-server-stream"))
        check(serverStream.receive()?.message == "hello, lifecycle-server-stream") {
            "lifecycle client iteration $iteration: server stream first response failed"
        }
        serverStream.close()

        val clientStream = client.lotsOfGreetingsCall()
        clientStream.send(request("cancelled-client-stream"))
        clientStream.close()

        val bidi = client.bidiHelloCall()
        bidi.send(request("cancelled-bidi"))
        check(bidi.receive()?.message == "echo, cancelled-bidi") {
            "lifecycle client iteration $iteration: bidi first response failed"
        }
        bidi.close()
    } finally {
        connection.shutdown()
    }
}

private data class ClientConnection(
    val transport: RpcTransport,
    val shutdown: suspend () -> Unit,
)

private suspend fun connect(options: CliOptions): ClientConnection {
    val config =
        NettyQuicClientConfig(
            remoteAddress = parseAddress(options.address),
            tls = NettyClientTls("localhost", trustCertificates = listOf(readCertificate(options.certificate))),
        )
    return when (options.transport) {
        ClientTransport.NATIVE -> {
            val transport = NettyQuicRpcTransport.connect(config)
            ClientConnection(transport, transport::shutdown)
        }

        ClientTransport.HTTP3 -> {
            val transport = NettyHttp3RpcTransport.connect(config)
            ClientConnection(transport, transport::shutdown)
        }
    }
}

internal fun parseAddress(value: String): InetSocketAddress {
    val separator = value.lastIndexOf(':')
    require(separator > 0 && separator < value.lastIndex) { "invalid -addr ${value.quoted()}" }
    val rawHost = value.substring(0, separator)
    val host = rawHost.removePrefix("[").removeSuffix("]")
    val port = value.substring(separator + 1).toIntOrNull()
    require(port != null && port in 0..65535) { "invalid -addr port in ${value.quoted()}" }
    return InetSocketAddress(host, port)
}

private fun formatAddress(address: InetSocketAddress): String {
    val host = address.address.hostAddress
    return if (host.contains(':')) "[$host]:${address.port}" else "$host:${address.port}"
}

private class LocalAdmission(
    configuredOrigins: Set<String>,
    configuredAuthorities: Set<String>,
) {
    private val origins =
        configuredOrigins.ifEmpty {
            setOf("http://127.0.0.1:8080", "http://localhost:8080")
        }
    private val configuredAuthorities = configuredAuthorities.toSet()
    private val authorities = AtomicReference<Set<String>>(emptySet())

    fun bound(address: InetSocketAddress) {
        val port = address.port
        val local =
            setOf(
                "127.0.0.1:$port",
                "localhost:$port",
                "[::1]:$port",
                formatAddress(address),
            )
        authorities.set(if (configuredAuthorities.isEmpty()) local else configuredAuthorities)
    }

    fun admitHttp3(request: zip.trev.trevrpc.netty.Http3AdmissionRequest): Boolean =
        request.secure && request.authority in authorities.get()

    fun admitWebTransport(request: zip.trev.trevrpc.netty.WebTransportAdmissionRequest): Boolean =
        request.secure && request.authority in authorities.get() && request.origin in origins
}

private fun commaSeparated(value: String?): Set<String> =
    value
        ?.split(',')
        ?.map(String::trim)
        ?.filter(String::isNotEmpty)
        ?.toSet()
        .orEmpty()

private fun String.quoted(): String = "\"$this\""
