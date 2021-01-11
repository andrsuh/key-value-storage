package ru.andrey.kvstorage;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import ru.andrey.kvstorage.resp.ByteToRespCommandDecoder;
import ru.andrey.kvstorage.resp.RespCommandToByteEncoder;
import ru.andrey.kvstorage.resp.object.RespArray;
import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.server.console.DatabaseCommand;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;
import ru.andrey.kvstorage.server.console.DatabaseCommands;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.console.impl.ExecutionEnvironmentImpl;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.initialization.Initializer;
import ru.andrey.kvstorage.server.initialization.impl.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DatabaseNettyServer {

    private static DatabaseNettyServer databaseServer;

    private final DatabaseServerBootstrap bs;
    private final ExecutionEnvironment env;


    public DatabaseNettyServer(ExecutionEnvironment env, Initializer initializer) throws DatabaseException, InterruptedException {
        this.env = env;
        this.bs = new DatabaseServerBootstrap(env);

        InitializationContextImpl initializationContext = InitializationContextImpl.builder()
                .executionEnvironment(env)
                .build();

        initializer.perform(initializationContext);
    }

    public static void main(String[] args) throws DatabaseException, InterruptedException {

        Initializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));

        databaseServer = new DatabaseNettyServer(new ExecutionEnvironmentImpl(), initializer);

         databaseServer.executeNextCommand("0 UPDATE_KEY test_3 Post 2 {\"title\":\"post\",\"user\":\"andrey\",\"content\":\"bla\"}");
         databaseServer.executeNextCommand("0 CREATE_DATABASE test_3");
//         databaseServer.executeNextCommand("0 CREATE_TABLE test_3 Post");
    }

    public DatabaseCommandResult executeNextCommand(String commandText) {
        try {
            if (StringUtils.isEmpty(commandText)) {
                return DatabaseCommandResult.error("Command name is not specified");
            }

            final String[] args = commandText.split(" ");
            if (args.length < 1) {
                return DatabaseCommandResult.error("Command name is not specified");
            }

            List<String> commandArgs = Arrays.stream(args).skip(1).collect(Collectors.toList());
            return DatabaseCommands.valueOf(args[0]).getCommand(env, commandArgs).execute();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return DatabaseCommandResult.error(e);
        }
    }

    private static class DatabaseServerBootstrap {
        private final EventLoopGroup bossGroup = new NioEventLoopGroup();
        private final EventLoopGroup workerGroup = new NioEventLoopGroup();
        private final ServerBootstrap b;

        ExecutionEnvironment env;

        public DatabaseServerBootstrap(ExecutionEnvironment env) throws InterruptedException {
            this.env = env;
//            try {
            b = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() { // (4)
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(
                                    new ByteToRespCommandDecoder(),
                                    new KvsServerInboundHandler(env),
                                    new RespCommandToByteEncoder());
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            // Bind and start to accept incoming connections.
            ChannelFuture f = b.bind("127.0.0.1", 8080).sync();

            System.out.println("Трррр Сервер стартанулллллллл");

            f.channel().closeFuture().addListener(new GenericFutureListener<Future<? super Void>>() {
                @Override
                public void operationComplete(Future<? super Void> future) throws Exception {
                    System.out.println("епац сессия серверная дропнулафсь");
                }
            });
//            }
//            finally {
//                workerGroup.shutdownGracefully();
//                bossGroup.shutdownGracefully();
//            }
        }
    }

    @AllArgsConstructor
    static class KvsServerInboundHandler extends ChannelInboundHandlerAdapter {
        ExecutionEnvironment env;

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            RespArray message = (RespArray) msg;
            System.out.println("SERVER GOT: " + message);

//            if (objects.isEmpty()) {
//                throw new IllegalArgumentException("Command name is not specified");
//            }

            final String[] args = message.getObjects().stream()
                    .map(RespObject::asString)
                    .toArray(String[]::new);

            List<String> commandArgs = Arrays.stream(args).skip(1).collect(Collectors.toList());

            DatabaseCommand command = DatabaseCommands.valueOf(args[1]).getCommand(env, commandArgs);

            DatabaseCommandResult databaseCommandResult = executeNextCommand(command);

            RespObject commandResult = databaseCommandResult.serialize();

            RespArray serverResponse = new RespArray(message.getObjects().get(0), commandResult);

            ctx.channel().writeAndFlush(serverResponse).addListener(new GenericFutureListener<Future<? super Void>>() {
                @Override
                public void operationComplete(Future<? super Void> future) throws Exception {
                    System.out.println("Опппана сообщение-то ушло!");
                }
            });
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            cause.printStackTrace(); //
        }

        public DatabaseCommandResult executeNextCommand(DatabaseCommand command) {
            try {
                return command.execute();
            } catch (Exception e) {
                System.out.println(e.getMessage());
                return DatabaseCommandResult.error(e);
            }
        }
    }
}
