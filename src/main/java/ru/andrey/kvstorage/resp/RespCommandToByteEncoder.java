package ru.andrey.kvstorage.resp;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import ru.andrey.kvstorage.resp.object.RespObject;

public class RespCommandToByteEncoder extends MessageToByteEncoder<RespObject> {
    @Override
    protected void encode(ChannelHandlerContext ctx, RespObject msg, ByteBuf out) throws Exception {
        System.out.println("опаньки сереализуем респ в байты");
        out.writeBytes(msg.getBytes());
    }
}
