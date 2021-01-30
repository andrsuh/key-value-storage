package ru.andrey.kvstorage.resp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class ByteToRespCommandDecoder extends ByteToMessageDecoder {
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    private final ByteBuf buf = Unpooled.buffer();
    private final RespByteBufferReader reader = new RespByteBufferReader(buf);

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        boolean aboutTheMessage = false;
        while (in.isReadable() || out.isEmpty()) {
            byte b = in.readByte();
            buf.writeByte(b);

            if (b == CR) {
                aboutTheMessage = true;
                continue;
            }

            if (aboutTheMessage) {
                if (b == LF) {
                    reader.readObject().ifPresent(out::add);
                }
                aboutTheMessage = false;
            }
        }
    }
}
