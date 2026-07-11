package zip.trev.trevrpc.examples

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
import java.io.StringWriter
import java.math.BigInteger
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyPairGenerator
import java.security.PrivateKey
import java.security.SecureRandom
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.security.spec.ECGenParameterSpec
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Date

internal data class TlsIdentity(
    val privateKey: PrivateKey,
    val certificate: X509Certificate,
)

internal fun generateLocalIdentity(): TlsIdentity {
    val keys =
        KeyPairGenerator.getInstance("EC").run {
            initialize(ECGenParameterSpec("secp256r1"), SecureRandom())
            generateKeyPair()
        }
    val now = Instant.now()
    val subject = X500Name("CN=localhost")
    val certificateBuilder =
        JcaX509v3CertificateBuilder(
            subject,
            BigInteger(160, SecureRandom()).abs(),
            Date.from(now.minus(1, ChronoUnit.HOURS)),
            Date.from(now.plus(24, ChronoUnit.HOURS)),
            subject,
            keys.public,
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
        GeneralNames(
            arrayOf(
                GeneralName(GeneralName.dNSName, "localhost"),
                GeneralName(GeneralName.iPAddress, "127.0.0.1"),
            ),
        ),
    )
    val signer = JcaContentSignerBuilder("SHA256withECDSA").build(keys.private)
    val certificate = JcaX509CertificateConverter().getCertificate(certificateBuilder.build(signer))
    certificate.verify(keys.public)
    return TlsIdentity(keys.private, certificate)
}

internal fun writeCertificate(
    path: Path,
    certificate: X509Certificate,
) {
    path.toAbsolutePath().parent?.let(Files::createDirectories)
    val pem = StringWriter().also { output -> JcaPEMWriter(output).use { it.writeObject(certificate) } }.toString()
    Files.writeString(path, pem)
}

internal fun readCertificate(path: Path): X509Certificate =
    Files.newInputStream(path).use { input ->
        CertificateFactory.getInstance("X.509").generateCertificate(input) as X509Certificate
    }
