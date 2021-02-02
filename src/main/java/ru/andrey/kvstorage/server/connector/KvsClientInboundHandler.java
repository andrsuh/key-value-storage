package ru.andrey.kvstorage.server.connector;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import ru.andrey.kvstorage.jclient.connection.ConnectionPool;
import ru.andrey.kvstorage.resp.object.RespArray;
import ru.andrey.kvstorage.resp.object.RespCommandId;
import ru.andrey.kvstorage.resp.object.RespObject;

import java.util.concurrent.atomic.AtomicReference;

public class KvsClientInboundHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        RespArray message = (RespArray) msg;
        System.out.println("Server  responded: [" + message.asString()  + "]");
        RespCommandId commandId = (RespCommandId) message.getObjects().get(0);


        AtomicReference<RespObject> resultHolder = ConnectionPool.results.get(commandId.commandId);
        if (resultHolder == null) {
            throw new IllegalArgumentException("There is no such command: " + commandId);
        }

        resultHolder.set(message.getObjects().get(1));
        synchronized (resultHolder) {
            resultHolder.notify();
        }

        System.out.println("Client got response to command id: " + commandId.commandId);
    }
}
