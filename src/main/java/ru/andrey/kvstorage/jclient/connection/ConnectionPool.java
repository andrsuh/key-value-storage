package ru.andrey.kvstorage.jclient.connection;


import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import ru.andrey.kvstorage.resp.ByteToRespDecoder;
import ru.andrey.kvstorage.resp.RespToByteEncoder;
import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.server.connector.KvsClientInboundHandler;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class ConnectionPool {

    private final List<NettyKvsConnection> connections;

    public static final Map<Integer, AtomicReference<RespObject>> results = new ConcurrentHashMap<>();

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

        private final Bootstrap bs = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                                .addLast(new RespToByteEncoder(),
                                        new ByteToRespDecoder(),
                                        new KvsClientInboundHandler()
                                );
                    }
                });

        public NettyClientBootstrap(ConnectionConfig config) {
            this.config = config;
        }

        private NettyKvsConnection initiateSession() {
//            try {
            ChannelFuture f = null;
            try {
                f = bs.connect(config.getHost(), config.getPort()).sync();
            } catch (InterruptedException e) {
                throw new IllegalArgumentException("Exception while creating connection");
            }
            return new NettyKvsConnection(f.channel());
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
                public void operationComplete(Future future) throws Exception {
                    System.out.println("SESSION CLOSED");
                }
            });
        }

        @Override
        public RespObject send(int commandId, RespObject command) {
            AtomicReference<RespObject> futureResult = results.computeIfAbsent(commandId, id -> new AtomicReference<>());
            try {
                synchronized (futureResult) {
                    channel.writeAndFlush(command);
                    futureResult.wait(); // todo sukhoa we can implement awaitility in temporary manner
                }
                return futureResult.get();
            } catch (Exception e) {
                throw new IllegalArgumentException("Interrupted: ", e);
            } finally {
                results.remove(commandId);
            }
        }

        @Override
        public void close() {
            channel.close().addListener(new GenericFutureListener<Future<? super Void>>() {
                @Override
                public void operationComplete(Future<? super Void> future) throws Exception {
                    System.out.println("REALLY SESSION CLOSED BY CLIENT");
                }
            });
        }
    }

}