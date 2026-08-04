package zip.trev.trevrpc.netty

import io.netty.channel.WriteBufferWaterMark
import io.netty.handler.codec.quic.QuicSslContext
import io.netty.handler.codec.quic.QuicSslContextBuilder
import io.netty.handler.codec.quic.QuicTokenHandler
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import io.netty.util.Mapping
import zip.trev.trevrpc.ALPN
import zip.trev.trevrpc.DEFAULT_MAX_FRAME_SIZE
import zip.trev.trevrpc.DEFAULT_MAX_STREAM_MESSAGES
import java.io.File
import java.net.InetSocketAddress
import java.security.PrivateKey
import java.security.cert.X509Certificate
import javax.net.ssl.TrustManagerFactory
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

const val HTTP3_ALPN: String = "h3"
const val HTTP3_PATH: String = "/trevrpc"
const val WEBTRANSPORT_PATH: String = "/trevrpc"
const val TREV_RPC_MEDIA_TYPE: String = "application/trevrpc"

data class NettyClientTls(
    val serverName: String,
    val trustManagerFactory: TrustManagerFactory? = null,
    val trustCertificates: List<X509Certificate> = emptyList(),
    val insecureDevTrust: Boolean = false,
    val verifyHostname: Boolean = true,
) {
    init {
        require(serverName.isNotBlank()) { "serverName must not be blank" }
        require(listOf(trustManagerFactory != null, trustCertificates.isNotEmpty(), insecureDevTrust).count { it } <= 1) {
            "configure only one of trustManagerFactory, trustCertificates, or insecureDevTrust"
        }
    }

    internal fun context(protocol: String): QuicSslContext {
        val builder = QuicSslContextBuilder.forClient().applicationProtocols(protocol)
        when {
            insecureDevTrust -> builder.trustManager(InsecureTrustManagerFactory.INSTANCE)
            trustManagerFactory != null -> builder.trustManager(trustManagerFactory)
            trustCertificates.isNotEmpty() -> builder.trustManager(*trustCertificates.toTypedArray())
        }
        builder.endpointIdentificationAlgorithm(if (verifyHostname) "HTTPS" else null)
        return builder.build()
    }
}

sealed interface NettyServerTls {
    fun context(protocols: Array<String>): QuicSslContext

    data class Pem(
        val privateKey: File,
        val certificateChain: File,
        val privateKeyPassword: String? = null,
    ) : NettyServerTls {
        override fun context(protocols: Array<String>): QuicSslContext =
            QuicSslContextBuilder
                .forServer(privateKey, privateKeyPassword, certificateChain)
                .applicationProtocols(*protocols)
                .build()
    }

    data class KeyAndCertificates(
        val privateKey: PrivateKey,
        val certificateChain: List<X509Certificate>,
        val privateKeyPassword: String? = null,
    ) : NettyServerTls {
        init {
            require(certificateChain.isNotEmpty()) { "certificateChain must not be empty" }
        }

        override fun context(protocols: Array<String>): QuicSslContext =
            QuicSslContextBuilder
                .forServer(privateKey, privateKeyPassword, *certificateChain.toTypedArray())
                .applicationProtocols(*protocols)
                .build()
    }

    data class Sni(
        val mapping: Mapping<in String, out QuicSslContext>,
    ) : NettyServerTls {
        override fun context(protocols: Array<String>): QuicSslContext =
            QuicSslContextBuilder.buildForServerWithSni { hostname ->
                mapping.map(hostname)?.also { context ->
                    require(context.applicationProtocolNegotiator().protocols().containsAll(protocols.asList())) {
                        "SNI context for $hostname must advertise ${protocols.joinToString()}"
                    }
                }
            }
    }
}

data class NettyTransportOptions(
    val maxFrameSize: Int = DEFAULT_MAX_FRAME_SIZE,
    val workerParallelism: Int = Runtime.getRuntime().availableProcessors().coerceAtLeast(2),
    val inboundQueueCapacity: Int = DEFAULT_MAX_STREAM_MESSAGES,
    val maxIdleTime: Duration = 30.seconds,
    val shutdownTimeout: Duration = 10.seconds,
    val writeBufferLowWaterMark: Int = 32 * 1024,
    val writeBufferHighWaterMark: Int = 64 * 1024,
) {
    init {
        require(maxFrameSize >= 0) { "maxFrameSize must be non-negative" }
        require(workerParallelism > 0) { "workerParallelism must be positive" }
        require(inboundQueueCapacity > 0) { "inboundQueueCapacity must be positive" }
        require(maxIdleTime.isFinite() && maxIdleTime.isPositive()) { "maxIdleTime must be positive and finite" }
        require(shutdownTimeout.isFinite() && shutdownTimeout.isPositive()) {
            "shutdownTimeout must be positive and finite"
        }
        require(writeBufferLowWaterMark >= 0) { "writeBufferLowWaterMark must be non-negative" }
        require(writeBufferHighWaterMark >= writeBufferLowWaterMark) {
            "writeBufferHighWaterMark must be at least writeBufferLowWaterMark"
        }
    }

    internal val waterMark: WriteBufferWaterMark
        get() = WriteBufferWaterMark(writeBufferLowWaterMark, writeBufferHighWaterMark)
}

data class NettyQuicClientConfig(
    val remoteAddress: InetSocketAddress,
    val tls: NettyClientTls,
    val options: NettyTransportOptions = NettyTransportOptions(),
)

data class Http3AdmissionRequest(
    val path: String,
    val method: String,
    val authority: String?,
    val secure: Boolean,
    val headers: Map<String, List<String>>,
    val protocol: String? = null,
)

fun interface Http3Admission {
    fun admit(request: Http3AdmissionRequest): Boolean
}

data class WebTransportAdmissionRequest(
    val path: String,
    val authority: String,
    val origin: String?,
    val secure: Boolean,
    val headers: Map<String, List<String>>,
)

fun interface WebTransportAdmission {
    fun admit(request: WebTransportAdmissionRequest): Boolean
}

data class NettyRpcServerConfig(
    val bindAddress: InetSocketAddress,
    val tls: NettyServerTls,
    val enableNative: Boolean = true,
    val enableHttp3: Boolean = true,
    val enableWebTransport: Boolean = false,
    val http3Admission: Http3Admission? = null,
    val webTransportAdmission: WebTransportAdmission? = null,
    val tokenHandler: QuicTokenHandler = HmacQuicTokenHandler(),
    val options: NettyTransportOptions = NettyTransportOptions(),
) {
    init {
        require(enableNative || enableHttp3 || enableWebTransport) { "at least one transport must be enabled" }
    }

    internal fun protocols(): Array<String> =
        buildList {
            if (enableNative) add(ALPN)
            if (enableHttp3 || enableWebTransport) add(HTTP3_ALPN)
        }.toTypedArray()
}
