package ru.andrey.kvstorage.jclient;


import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import ru.andrey.kvstorage.KvsClientInboundHandler;
import ru.andrey.kvstorage.jclient.connection.KvsConnection;
import ru.andrey.kvstorage.jclient.exception.KvsConnectionException;
import ru.andrey.kvstorage.resp.ByteToRespCommandDecoder;
import ru.andrey.kvstorage.resp.RespCommandToByteEncoder;
import ru.andrey.kvstorage.resp.object.RespObject;

import java.util.List;
import java.util.Random;

public class SessionsPool {

    private final List<Session> sessions;

    private final Random random = new Random();

    public SessionsPool() throws InterruptedException {
        ClientBootstrap bootstrap = new ClientBootstrap();
        sessions = List.of(bootstrap.initiateSession());
    }

    public Session getClientSession() {
        return sessions.get(random.nextInt(100_000) % sessions.size());
    }

    static class ClientBootstrap {
        String host = "127.0.0.1"; // todo sukhoa

        int port = Integer.parseInt("8080");

        final EventLoopGroup workerGroup = new NioEventLoopGroup();

        final Bootstrap bs = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                                .addLast(new RespCommandToByteEncoder(),
                                        new ByteToRespCommandDecoder(),
                                        new KvsClientInboundHandler()
                                );
                    }
                });

        private Session initiateSession() throws InterruptedException {
//            try {
            ChannelFuture f = bs.connect(host, port).sync();
            return new Session(f.channel());
//            } finally {
//                workerGroup.shutdownGracefully();
//            }
        }
    }

    public static class Session implements KvsConnection {
        private final Channel channel;

        public Session(Channel channel) {
            this.channel = channel;
            channel.closeFuture().addListener(new GenericFutureListener() {
                @Override
                public void operationComplete(Future future) throws Exception {
                    System.out.println("SESSION CLOSED");
                }
            });
        }

        @Override
        public RespObject send(RespObject command) {
            channel.writeAndFlush(command);
            return null;
        }

        @Override
        public void close() throws KvsConnectionException {
            channel.close().addListener(new GenericFutureListener<Future<? super Void>>() {
                @Override
                public void operationComplete(Future<? super Void> future) throws Exception {
                    System.out.println("REALLY SESSION CLOSED BY CLIENT");
                }
            });
        }
    }

}