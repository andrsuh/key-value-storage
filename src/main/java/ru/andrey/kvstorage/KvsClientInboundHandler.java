package ru.andrey.kvstorage;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;
import ru.andrey.kvstorage.jclient.client.SimpleKvsClient;
import ru.andrey.kvstorage.resp.object.RespArray;
import ru.andrey.kvstorage.resp.object.RespCommandId;

@Slf4j
public class KvsClientInboundHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        RespArray message = (RespArray) msg;
        log.info("Client got {}", message.asString());
        RespCommandId respId = (RespCommandId) message.getObjects().get(0);

        SimpleKvsClient.responses.put(respId.commandId, message.getObjects().get(1));

        Object monitor = SimpleKvsClient.requests.get(respId.commandId);
        if (monitor != null) {
            synchronized (monitor) {
                monitor.notify();
            }
        } else {
            log.warn("Duplicates ёмаё");
        }

        log.info("Got answer on {}", respId.commandId);
    }
}
