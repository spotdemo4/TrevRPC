package zip.trev.trevrpc.bench

import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x509.BasicConstraints
import org.bouncycastle.asn1.x509.ExtendedKeyUsage
import org.bouncycastle.asn1.x509.Extension
import org.bouncycastle.asn1.x509.GeneralName
import org.bouncycastle.asn1.x509.GeneralNames
import org.bouncycastle.asn1.x509.KeyPurposeId
import org.bouncycastle.asn1.x509.KeyUsage
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder
import org.bouncycastle.openssl.jcajce.JcaPEMWriter
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder
import org.bouncycastle.util.io.pem.PemObject
import java.math.BigInteger
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.PrivateKey
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.security.spec.ECGenParameterSpec
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Date

internal class TestTlsAuthority private constructor(
    private val directory: Path,
    private val keyPair: KeyPair,
    private val subject: X500Name,
    val certificate: X509Certificate,
    val certificatePath: Path,
) : AutoCloseable {
    fun issueServer(
        name: String,
        sanType: Int,
    ): TestServerIdentity {
        val serverKeys = generateKeys()
        val now = Instant.now()
        val certificateBuilder =
            JcaX509v3CertificateBuilder(
                subject,
                serialNumber(),
                Date.from(now.minus(1, ChronoUnit.HOURS)),
                Date.from(now.plus(24, ChronoUnit.HOURS)),
                X500Name("CN=$name"),
                serverKeys.public,
            )
        certificateBuilder.addExtension(Extension.basicConstraints, true, BasicConstraints(false))
        certificateBuilder.addExtension(Extension.keyUsage, true, KeyUsage(KeyUsage.digitalSignature))
        certificateBuilder.addExtension(
            Extension.extendedKeyUsage,
            false,
            ExtendedKeyUsage(KeyPurposeId.id_kp_serverAuth),
        )
        certificateBuilder.addExtension(
            Extension.subjectAlternativeName,
            false,
            GeneralNames(GeneralName(sanType, name)),
        )
        val signer = JcaContentSignerBuilder("SHA256withECDSA").build(keyPair.private)
        val certificate = JcaX509CertificateConverter().getCertificate(certificateBuilder.build(signer))
        certificate.verify(keyPair.public)

        val identityDirectory = Files.createDirectory(directory.resolve("server-${serialNumber()}"))
        val certificatePath = identityDirectory.resolve("certificate.pem")
        val privateKeyPath = identityDirectory.resolve("private-key.pem")
        writePem(certificatePath, certificate, this.certificate)
        writePem(privateKeyPath, PemObject("PRIVATE KEY", serverKeys.private.encoded))
        return TestServerIdentity(
            privateKey = serverKeys.private,
            certificateChain = listOf(certificate, this.certificate),
            certificatePath = certificatePath,
            privateKeyPath = privateKeyPath,
        )
    }

    override fun close() {
        check(directory.toFile().deleteRecursively()) { "failed to delete test TLS directory $directory" }
    }

    companion object {
        fun create(): TestTlsAuthority {
            val directory = Files.createTempDirectory("trevrpc-test-ca-")
            val keyPair = generateKeys()
            val subject = X500Name("CN=TrevRPC test CA")
            val now = Instant.now()
            val certificateBuilder =
                JcaX509v3CertificateBuilder(
                    subject,
                    serialNumber(),
                    Date.from(now.minus(1, ChronoUnit.HOURS)),
                    Date.from(now.plus(24, ChronoUnit.HOURS)),
                    subject,
                    keyPair.public,
                )
            certificateBuilder.addExtension(Extension.basicConstraints, true, BasicConstraints(true))
            certificateBuilder.addExtension(
                Extension.keyUsage,
                true,
                KeyUsage(KeyUsage.keyCertSign or KeyUsage.cRLSign),
            )
            val signer = JcaContentSignerBuilder("SHA256withECDSA").build(keyPair.private)
            val certificate = JcaX509CertificateConverter().getCertificate(certificateBuilder.build(signer))
            certificate.verify(keyPair.public)
            val certificatePath = directory.resolve("ca.pem")
            writePem(certificatePath, certificate)
            return TestTlsAuthority(directory, keyPair, subject, certificate, certificatePath)
        }
    }
}

internal data class TestServerIdentity(
    val privateKey: PrivateKey,
    val certificateChain: List<X509Certificate>,
    val certificatePath: Path,
    val privateKeyPath: Path,
)

internal const val TEST_SERVER_IP: String = "127.0.0.1"

private fun generateKeys(): KeyPair =
    KeyPairGenerator.getInstance("EC").run {
        initialize(ECGenParameterSpec("secp256r1"), SecureRandom())
        generateKeyPair()
    }

private fun serialNumber(): BigInteger = BigInteger(160, SecureRandom()).abs()

private fun writePem(
    path: Path,
    vararg values: Any,
) {
    JcaPEMWriter(Files.newBufferedWriter(path)).use { output ->
        values.forEach(output::writeObject)
    }
}
