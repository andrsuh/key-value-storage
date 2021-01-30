package ru.andrey.kvstorage.resp;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import ru.andrey.kvstorage.resp.object.RespObject;

public class RespToByteEncoder extends MessageToByteEncoder<RespObject> {
    @Override
    protected void encode(ChannelHandlerContext ctx, RespObject msg, ByteBuf out) throws Exception {
        byte[] msgBytes = msg.getBytes();
        System.out.println("Serializing: " + msg + ", bytes num :" + msgBytes.length);
        out.writeBytes(msgBytes);
    }
}
