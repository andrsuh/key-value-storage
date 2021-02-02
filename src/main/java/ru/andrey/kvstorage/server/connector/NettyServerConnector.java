package ru.andrey.kvstorage.server.connector;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.AllArgsConstructor;
import ru.andrey.kvstorage.resp.ByteToRespDecoder;
import ru.andrey.kvstorage.resp.RespToByteEncoder;
import ru.andrey.kvstorage.resp.object.RespArray;
import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.server.DatabaseServer;
import ru.andrey.kvstorage.server.config.ConfigLoader;
import ru.andrey.kvstorage.server.config.DatabaseServerConfig;
import ru.andrey.kvstorage.server.config.ServerConfig;
import ru.andrey.kvstorage.server.console.DatabaseCommand;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;
import ru.andrey.kvstorage.server.console.DatabaseCommands;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.console.impl.ExecutionEnvironmentImpl;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseInitializer;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseServerInitializer;
import ru.andrey.kvstorage.server.initialization.impl.SegmentInitializer;
import ru.andrey.kvstorage.server.initialization.impl.TableInitializer;

import java.util.List;

import static ru.andrey.kvstorage.server.console.DatabaseCommandArgPositions.COMMAND_NAME;

public class NettyServerConnector {

    private final DatabaseServerBootstrap bs;
    private final DatabaseServer databaseServer;

    public NettyServerConnector(DatabaseServer databaseServer, ServerConfig config) throws InterruptedException {
        this.databaseServer = databaseServer;
        this.bs = new DatabaseServerBootstrap(config, databaseServer.getEnv());
    }

    private static class DatabaseServerBootstrap implements AutoCloseable {
        private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        private final EventLoopGroup workerGroup = new NioEventLoopGroup(1);
        private final ServerBootstrap b;

        public DatabaseServerBootstrap(ServerConfig config, ExecutionEnvironment env) throws InterruptedException {
            b = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() { // (4)
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(
                                    new ByteToRespDecoder(),
                                    new KvsServerInboundHandler(env),
                                    new RespToByteEncoder());
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            // Bind and start to accept incoming connections.
            System.out.println("Starting server on port " + config.getPort() + " (host " + config.getHost() + ")");
            ChannelFuture f = b.bind(config.getHost(), config.getPort()).sync();

            System.out.println("Netty server started.");

            f.channel().closeFuture().addListener(future -> System.out.println("Server shut down server acceptor closed."));
        }

        @Override
        public void close() {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
            System.out.println("Netty server stopped.");
        }
    }

    public void close() {
        bs.close();
    }

    @AllArgsConstructor
    static class KvsServerInboundHandler extends ChannelInboundHandlerAdapter {
        private final ExecutionEnvironment env;

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            RespArray message = (RespArray) msg;
            System.out.println("Server got client request: [ " + message + "]");

            List<RespObject> commandArgs = message.getObjects();

            DatabaseCommand command = DatabaseCommands
                    .valueOf(commandArgs.get(COMMAND_NAME.getPositionIndex()).asString())
                    .getCommand(env, commandArgs);

            DatabaseCommandResult databaseCommandResult = executeNextCommand(command);

            RespObject commandResult = databaseCommandResult.serialize();

            RespArray serverResponse = new RespArray(message.getObjects().get(0), commandResult);

            ctx.channel().writeAndFlush(serverResponse).addListener(future -> System.out.println("Server sent: " + serverResponse));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
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

    public static void main(String[] args) throws InterruptedException, DatabaseException {
        ConfigLoader loader = new ConfigLoader();
        DatabaseServerConfig config = loader.readConfig();

        DatabaseServerInitializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));

        DatabaseServer databaseServer = new DatabaseServer(new ExecutionEnvironmentImpl(config.getDbConfig()), initializer);

        new NettyServerConnector(databaseServer, config.getServerConfig());
    }
}
