package zip.trev.trevrpc.netty

import io.netty.buffer.ByteBuf
import io.netty.handler.codec.quic.QuicTokenHandler
import java.net.InetSocketAddress
import java.security.MessageDigest
import java.security.SecureRandom
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

class HmacQuicTokenHandler(
    secret: ByteArray = ByteArray(32).also(SecureRandom()::nextBytes),
    private val validity: Duration = 5.minutes,
) : QuicTokenHandler {
    private val key = SecretKeySpec(secret.copyOf(), "HmacSHA256")

    init {
        require(secret.size >= 32) { "QUIC token secret must contain at least 32 bytes" }
        require(validity.isPositive()) { "QUIC token validity must be positive" }
    }

    override fun writeToken(
        output: ByteBuf,
        destinationConnectionId: ByteBuf,
        address: InetSocketAddress,
    ): Boolean {
        val timestamp = System.currentTimeMillis()
        val addressBytes = address.address.address
        val connectionId =
            ByteArray(destinationConnectionId.readableBytes()).also {
                destinationConnectionId.getBytes(destinationConnectionId.readerIndex(), it)
            }
        val signature = sign(timestamp, addressBytes, connectionId)
        output.writeLong(timestamp)
        output.writeBytes(addressBytes)
        output.writeBytes(signature)
        output.writeBytes(connectionId)
        return true
    }

    override fun validateToken(
        token: ByteBuf,
        address: InetSocketAddress,
    ): Int {
        val addressBytes = address.address.address
        val prefixLength = Long.SIZE_BYTES + addressBytes.size + SIGNATURE_SIZE
        if (token.readableBytes() <= prefixLength) return -1
        val start = token.readerIndex()
        val timestamp = token.getLong(start)
        val age = System.currentTimeMillis() - timestamp
        if (age < 0 || age > validity.inWholeMilliseconds) return -1
        val encodedAddress = ByteArray(addressBytes.size)
        token.getBytes(start + Long.SIZE_BYTES, encodedAddress)
        if (!MessageDigest.isEqual(encodedAddress, addressBytes)) return -1
        val signature = ByteArray(SIGNATURE_SIZE)
        token.getBytes(start + Long.SIZE_BYTES + addressBytes.size, signature)
        val connectionIdLength = token.readableBytes() - prefixLength
        val connectionId = ByteArray(connectionIdLength)
        token.getBytes(start + prefixLength, connectionId)
        val expected = sign(timestamp, addressBytes, connectionId)
        return if (MessageDigest.isEqual(signature, expected)) prefixLength else -1
    }

    override fun maxTokenLength(): Int = Long.SIZE_BYTES + 16 + SIGNATURE_SIZE + 20

    private fun sign(
        timestamp: Long,
        address: ByteArray,
        connectionId: ByteArray,
    ): ByteArray =
        Mac.getInstance("HmacSHA256").run {
            init(key)
            update(ByteArray(Long.SIZE_BYTES) { shift -> (timestamp ushr ((7 - shift) * 8)).toByte() })
            update(address)
            doFinal(connectionId)
        }

    private companion object {
        const val SIGNATURE_SIZE = 32
    }
}
