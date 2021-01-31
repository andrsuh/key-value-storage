package ru.andrey.kvstorage.server;

import ru.andrey.kvstorage.resp.RespReader;
import ru.andrey.kvstorage.resp.RespWriter;
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
import ru.andrey.kvstorage.server.resp.CommandReader;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.*;

import static ru.andrey.kvstorage.server.console.DatabaseCommandArgPositions.COMMAND_NAME;

public class DatabaseServer implements AutoCloseable {

    private static ExecutorService clientIOWorkers = new ThreadPoolExecutor(
            100, 100, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1),
            (rejected, pool) -> {
                System.out.println("Client connection has been rejected. Number of clients exceeded: " + pool.getTaskCount());
            });

    private final ExecutorService connectionAcceptorExecutor = Executors.newSingleThreadExecutor();
    private final ExecutorService dbCommandExecutor = Executors.newSingleThreadExecutor();
    private final ServerSocket serverSocket;
    private final ExecutionEnvironment env;

    public DatabaseServer(ExecutionEnvironment env, Initializer initializer) throws IOException, DatabaseException {
        this.serverSocket = new ServerSocket(8080);
        this.env = env;

        InitializationContextImpl initializationContext = InitializationContextImpl.builder()
                .executionEnvironment(env)
                .build();

        initializer.perform(initializationContext);

        connectionAcceptorExecutor.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Socket clientSocket = DatabaseServer.this.serverSocket.accept();
                    clientIOWorkers.submit(new ClientTask(clientSocket, this));
                } catch (Throwable t) {
                    System.out.println("Server acceptor thread exception: " + t);
                }
            }
            System.out.println("Server acceptor stopped.");
        });
    }

    @Override
    public void close() throws Exception {
        connectionAcceptorExecutor.shutdownNow();
        dbCommandExecutor.shutdownNow();
        clientIOWorkers.shutdownNow();
        serverSocket.close();
    }

    public static void main(String[] args) throws IOException, DatabaseException {

        Initializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));

        DatabaseServer databaseServer = new DatabaseServer(new ExecutionEnvironmentImpl(), initializer);

        // databaseServer.executeNextCommand("SET_KEY test_3 Post 1 {\"title\":\"post\",\"user\":\"andrey\",\"content\":\"bla\"}");
    }

    public DatabaseCommandResult executeNextCommand(RespObject msg) {
        try {
            RespArray message = (RespArray) msg;
            System.out.println("Server got client request: [ " + message + "]");

            List<RespObject> commandArgs = message.getObjects();

            DatabaseCommand command = DatabaseCommands
                    .valueOf(commandArgs.get(COMMAND_NAME.getPositionIndex()).asString())
                    .getCommand(env, commandArgs);

            return command.execute();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return DatabaseCommandResult.error(e);
        }
    }

    public DatabaseCommandResult executeNextCommand(DatabaseCommand command) {
        try {
            return command.execute();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return DatabaseCommandResult.error(e);
        }
    }

    static class ClientTask implements Runnable, Closeable {
        private final Socket client;
        private final DatabaseServer server;

        public ClientTask(Socket client, DatabaseServer server) {
            this.client = client;
            this.server = server;
        }

        @Override
        public void run() {
            try {
                final CommandReader reader = new CommandReader(
                        new RespReader(new BufferedInputStream(client.getInputStream())),
                        server.env
                );
                final RespWriter writer = new RespWriter(new BufferedOutputStream(client.getOutputStream()));

                while (!client.isClosed() && !Thread.currentThread().isInterrupted()) {
                    if (!reader.hasNextCommand()) continue;

                    DatabaseCommand databaseCommand = reader.readCommand();

                    DatabaseCommandResult databaseCommandResult = server.dbCommandExecutor.submit(() -> server.executeNextCommand(databaseCommand)).get();

                    writer.write(databaseCommandResult.serialize());
                }
            } catch (IOException e) {
                System.out.println("Client socket threw IO Exception " + e.getMessage());
            } catch (InterruptedException e) {
                System.out.println("Got interrupted exception " + e.getMessage()); // todo sukhoa bad
            } catch (ExecutionException e) {
                System.out.println("Got execution exception " + e.getMessage()); // todo sukhoa bad
            } catch (Throwable t) {
                System.out.println("Unexpected exception" + t);
            } finally {
                close();
            }
        }

        @Override
        public void close() {
            try {
                client.close();
                System.out.println("Client has been disconnected: " + client.getInetAddress());
            } catch (IOException e) {
                e.printStackTrace(); // todo sukhoa mmmmm
            }
        }
    }
}
