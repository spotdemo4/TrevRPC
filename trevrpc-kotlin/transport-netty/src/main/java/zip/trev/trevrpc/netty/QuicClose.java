package zip.trev.trevrpc.netty;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.util.CharsetUtil;

final class QuicClose {
    private QuicClose() { }

    static void applicationClose(QuicChannel channel, int error, String reason) {
        ByteBuf buffer = channel.alloc().buffer(reason.length());
        buffer.writeCharSequence(reason, CharsetUtil.UTF_8);
        try {
            channel.close(true, error, buffer);
        } catch (Throwable cause) {
            buffer.release();
            throw cause;
        }
    }
}
