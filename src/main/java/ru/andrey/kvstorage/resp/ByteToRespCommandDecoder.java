package ru.andrey.kvstorage.resp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class ByteToRespCommandDecoder extends ByteToMessageDecoder {

    private final ByteBuf byteBuffer = Unpooled.buffer();
    private final RespByteBufferReader reader = new RespByteBufferReader(byteBuffer);

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        log.info("Server got {}", in.readableBytes());

        boolean aboutTheMessage = false;
        while (in.isReadable() || out.isEmpty()) {
            byte readByte = in.readByte();
            byteBuffer.writeByte(readByte);

            if (readByte == CommandByte.CR.getSymbolByte()) {
                aboutTheMessage = true;
                continue;
            }

            if (aboutTheMessage) {
                if (readByte == CommandByte.LF.getSymbolByte()) {
                    reader.readObject().ifPresent(out::add);
                }
                aboutTheMessage = false;
            }
        }
    }
}
