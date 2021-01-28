package ru.andrey.kvstorage;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import ru.andrey.kvstorage.jclient.client.SimpleKvsClient;
import ru.andrey.kvstorage.resp.object.RespArray;
import ru.andrey.kvstorage.resp.object.RespCommandId;

public class KvsClientInboundHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        RespArray message = (RespArray) msg;
        System.out.println("CLIENT GOT: " + message.asString());
        RespCommandId respId = (RespCommandId) message.getObjects().get(0);

        SimpleKvsClient.responses.put(respId.commandId, message.getObjects().get(1));

        Object monitor = SimpleKvsClient.requests.get(respId.commandId);
        if (monitor != null) {
            synchronized (monitor) {
                monitor.notify();
            }
        } else {
            System.out.println("емае дупликаты");
        }

        System.out.println("Получили ответик на : " + respId.commandId);
    }
}
