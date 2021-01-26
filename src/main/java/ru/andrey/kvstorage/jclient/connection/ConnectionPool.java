package ru.andrey.kvstorage.jclient.connection;


import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import lombok.extern.slf4j.Slf4j;
import ru.andrey.kvstorage.KvsClientInboundHandler;
import ru.andrey.kvstorage.jclient.exception.KvsConnectionException;
import ru.andrey.kvstorage.resp.ByteToRespCommandDecoder;
import ru.andrey.kvstorage.resp.RespCommandToByteEncoder;
import ru.andrey.kvstorage.resp.object.RespObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

@Slf4j
public class ConnectionPool {

    private final List<NettyKvsConnection> connections;

    private final Random random = new Random();

    public ConnectionPool(ConnectionConfig config) {
        NettyClientBootstrap bootstrap = new NettyClientBootstrap(config);

        ArrayList<NettyKvsConnection> connections = new ArrayList<>();
        for (int i = 0; i < config.getPoolSize(); i++) {
            connections.add(bootstrap.initiateSession());
        }
        this.connections = Collections.unmodifiableList(connections);
    }

    public KvsConnection getClientConnection() {
        return connections.get(random.nextInt(100_000) % connections.size());
    }

    static class NettyClientBootstrap {
        private final ConnectionConfig config;

        private final EventLoopGroup workerGroup = new NioEventLoopGroup();

        private final Bootstrap bootstrap = new Bootstrap()
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

        public NettyClientBootstrap(ConnectionConfig config) {
            this.config = config;
        }

        private NettyKvsConnection initiateSession() {
//            try {
            log.info("Creating connection pool");
            ChannelFuture channelFuture;
            try {
                channelFuture = bootstrap.connect(config.getHost(), config.getPort()).sync();
            } catch (InterruptedException e) {
                log.error("Exception while creating connection", e);
                throw new IllegalArgumentException("Exception while creating connection");
            }
            return new NettyKvsConnection(channelFuture.channel());
//            } finally {
//                workerGroup.shutdownGracefully();
//            }
        }
    }

    public static class NettyKvsConnection implements KvsConnection {
        private final Channel channel;

        public NettyKvsConnection(Channel channel) {
            this.channel = channel;
            channel.closeFuture().addListener(new GenericFutureListener() {
                @Override
                public void operationComplete(Future future) {
                    log.info("Session closed");
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
                    log.info("Session closed by client");
                }
            });
        }
    }

}