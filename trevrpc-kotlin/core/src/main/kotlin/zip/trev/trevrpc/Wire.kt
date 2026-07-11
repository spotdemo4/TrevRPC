package zip.trev.trevrpc

enum class RpcKind(
    val value: Int,
) {
    UNARY(0),
    CLIENT_STREAMING(1),
    SERVER_STREAMING(2),
    BIDIRECTIONAL_STREAMING(3),
    ;

    companion object {
        fun fromValue(value: Int): RpcKind? = entries.firstOrNull { it.value == value }
    }
}

enum class RpcStreamFrameKind(
    val value: Int,
) {
    MESSAGE(0),
    STATUS(1),
    ;

    companion object {
        fun fromValue(value: Int): RpcStreamFrameKind? = entries.firstOrNull { it.value == value }
    }
}

data class RpcRequest(
    val service: String,
    val method: String,
    val body: ByteArray = byteArrayOf(),
    val metadata: Metadata = Metadata.EMPTY,
    val kindValue: Int = RpcKind.UNARY.value,
    val version: UInt = WIRE_VERSION,
    val timeoutNanos: ULong = 0uL,
) {
    val kind: RpcKind?
        get() = RpcKind.fromValue(kindValue)

    fun validateProtocol() {
        if (version != WIRE_VERSION) {
            throw TrevRpcException(
                Status.failedPrecondition("unsupported TrevRPC wire version $version; expected $WIRE_VERSION"),
            )
        }
        if (kind == null) {
            throw TrevRpcException(Status.invalidArgument("unsupported TrevRPC RPC kind $kindValue"))
        }
    }
}

data class RpcResponse(
    val statusValue: UInt = Code.OK.value,
    val message: String = "",
    val body: ByteArray = byteArrayOf(),
    val metadata: Metadata = Metadata.EMPTY,
) {
    val status: Status
        get() = Status(Code.fromValue(statusValue), message, metadata)

    companion object {
        fun ok(
            body: ByteArray,
            metadata: Metadata = Metadata.EMPTY,
        ): RpcResponse = RpcResponse(body = body.copyOf(), metadata = metadata)

        fun fromStatus(status: Status): RpcResponse = RpcResponse(status.code.value, status.message, metadata = status.metadata)
    }
}

data class RpcStreamFrame(
    val kindValue: Int = RpcStreamFrameKind.MESSAGE.value,
    val statusValue: UInt = Code.OK.value,
    val message: String = "",
    val body: ByteArray = byteArrayOf(),
    val metadata: Metadata = Metadata.EMPTY,
) {
    val kind: RpcStreamFrameKind?
        get() = RpcStreamFrameKind.fromValue(kindValue)

    val status: Status
        get() = Status(Code.fromValue(statusValue), message, metadata)

    companion object {
        fun message(body: ByteArray): RpcStreamFrame = RpcStreamFrame(body = body.copyOf())

        fun status(status: Status): RpcStreamFrame =
            RpcStreamFrame(
                kindValue = RpcStreamFrameKind.STATUS.value,
                statusValue = status.code.value,
                message = status.message,
                metadata = status.metadata,
            )
    }
}
