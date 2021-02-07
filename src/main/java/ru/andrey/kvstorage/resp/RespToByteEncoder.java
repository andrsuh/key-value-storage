package ru.andrey.kvstorage.resp;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.extern.slf4j.Slf4j;
import ru.andrey.kvstorage.resp.object.RespObject;

@Slf4j
public class RespToByteEncoder extends MessageToByteEncoder<RespObject> {
    @Override
    protected void encode(ChannelHandlerContext ctx, RespObject msg, ByteBuf out) throws Exception {
        byte[] msgBytes = msg.getBytes();
        log.debug("Serializing: {}, bytes num: {}", msg, msgBytes.length);
        out.writeBytes(msgBytes);
    }
}
