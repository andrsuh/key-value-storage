package ru.andrey.kvstorage.server.connector;

import ru.andrey.kvstorage.resp.RespReader;
import ru.andrey.kvstorage.resp.RespWriter;
import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.server.DatabaseServer;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;
import ru.andrey.kvstorage.server.console.impl.ExecutionEnvironmentImpl;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseInitializer;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseServerInitializer;
import ru.andrey.kvstorage.server.initialization.impl.SegmentInitializer;
import ru.andrey.kvstorage.server.initialization.impl.TableInitializer;
import ru.andrey.kvstorage.server.resp.CommandReader;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.*;

public class JavaSocketServerConnector implements AutoCloseable {

    private static final ExecutorService clientIOWorkers = new ThreadPoolExecutor(
            100, 100, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1),
            (rejected, pool) -> {
                System.out.println("Client connection has been rejected. Number of clients exceeded: " + pool.getTaskCount());
            });

    private final ExecutorService connectionAcceptorExecutor = Executors.newSingleThreadExecutor();
    private final ServerSocket serverSocket;
    private final DatabaseServer databaseServer;

    public JavaSocketServerConnector(DatabaseServer databaseServer) throws IOException, DatabaseException {
        this.databaseServer = databaseServer;
        this.serverSocket = new ServerSocket(8080);

        connectionAcceptorExecutor.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Socket clientSocket = JavaSocketServerConnector.this.serverSocket.accept();
                    clientIOWorkers.submit(new ClientTask(clientSocket, databaseServer));
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
        clientIOWorkers.shutdownNow();
        serverSocket.close();
    }

    public static void main(String[] args) throws IOException, DatabaseException {

        DatabaseServerInitializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));

        DatabaseServer databaseServer = new DatabaseServer(new ExecutionEnvironmentImpl(), initializer);

        new JavaSocketServerConnector(databaseServer);
    }

    public DatabaseCommandResult executeNextCommand(RespObject msg) {
        try {
            System.out.println("Server got client request: [ " + msg + "]");
            return databaseServer.executeNextCommand(msg).get();
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
                        server.getEnv()
                );
                final RespWriter writer = new RespWriter(new BufferedOutputStream(client.getOutputStream()));

                while (!client.isClosed() && !Thread.currentThread().isInterrupted()) {
                    if (!reader.hasNextCommand()) continue;

                    DatabaseCommandResult databaseCommandResult = server.executeNextCommand(reader.readCommand()).get();

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
