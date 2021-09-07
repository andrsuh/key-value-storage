package ru.andrey.kvstorage.server.connector;

import ru.andrey.kvstorage.resp.RespReader;
import ru.andrey.kvstorage.resp.RespWriter;
import ru.andrey.kvstorage.server.DatabaseServer;
import ru.andrey.kvstorage.server.config.ConfigLoader;
import ru.andrey.kvstorage.server.config.DatabaseServerConfig;
import ru.andrey.kvstorage.server.config.ServerConfig;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;
import ru.andrey.kvstorage.server.console.impl.ExecutionEnvironmentImpl;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseInitializer;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseServerInitializer;
import ru.andrey.kvstorage.server.initialization.impl.SegmentInitializer;
import ru.andrey.kvstorage.server.initialization.impl.TableInitializer;
import ru.andrey.kvstorage.server.resp.CommandReader;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Класс, который предоставляет доступ к серверу через сокеты
 */
public class JavaSocketServerConnector implements Closeable {

    /**
     * Экзекьютор для выполнения ClientTask
     */
    private final ExecutorService clientIOWorkers = Executors.newSingleThreadExecutor();
    private final ServerSocket serverSocket;
    private final ExecutorService connectionAcceptorExecutor = Executors.newSingleThreadExecutor();
    private final DatabaseServer databaseServer;

    /**
     * Стартует сервер. По аналогии с сокетом открывает коннекшн в конструкторе.
     */
    public JavaSocketServerConnector(DatabaseServer databaseServer, ServerConfig config) throws IOException {
        this.databaseServer = databaseServer;

        System.out.println("Starting server on port " + config.getPort() + " (host " + config.getHost() + ")");
        this.serverSocket = new ServerSocket(config.getPort());
        System.out.println("Using port " + serverSocket.getLocalPort() + " of localhost");
    }

    /**
     * Начинает слушать заданный порт, начинает аксептить клиентские сокеты. На каждый из них начинает клиентскую таску
     */
    public void start() {
        connectionAcceptorExecutor.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    clientIOWorkers.submit(new ClientTask(clientSocket, databaseServer));
                } catch (Throwable t) {
                    System.out.println("Server acceptor thread exception: " + t);
                }
            }
            System.out.println("Server acceptor stopped.");
        });
    }

    /**
     * Закрывает все, что нужно ¯\_(ツ)_/¯
     */
    @Override
    public void close() {
        System.out.println("Stopping socket connector");
        clientIOWorkers.shutdownNow();
        connectionAcceptorExecutor.shutdownNow();
        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws Exception {

        ConfigLoader loader = new ConfigLoader();
        DatabaseServerConfig config = loader.readConfig();

        DatabaseServerInitializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));

        DatabaseServer databaseServer = DatabaseServer.initialize(new ExecutionEnvironmentImpl(config.getDbConfig()), initializer);

        JavaSocketServerConnector javaSocketServerConnector = new JavaSocketServerConnector(databaseServer, config.getServerConfig());
        javaSocketServerConnector.start();

        Thread.sleep(120_0);

        javaSocketServerConnector.close();
    }

    /**
     * Runnable, описывающий исполнение клиентской команды.
     */
    static class ClientTask implements Runnable, Closeable {
        private final Socket client;
        private final DatabaseServer server;

        /**
         * @param client клиентский сокет
         * @param server сервер, на котором исполняется задача
         */
        public ClientTask(Socket client, DatabaseServer server) {
            this.client = client;
            this.server = server;
        }

        /**
         * Исполняет задачу.
         * 1. Читает из сокета команду с помощью {@link CommandReader}
         * 2. Исполняет ее на сервере
         * 3. Записывает результат в сокет с помощью {@link RespWriter}
         */
        @Override
        public void run() {
            try (CommandReader reader = new CommandReader(new RespReader(new BufferedInputStream(client.getInputStream())), server.getEnv());
                 RespWriter writer = new RespWriter(new BufferedOutputStream(client.getOutputStream()))) {
                while (!client.isClosed() && !Thread.currentThread().isInterrupted()) {
                    try {
                        if (!reader.hasNextCommand())
                            continue;
                    } catch (EOFException eof) {
                        break;
                    }

                    DatabaseCommandResult databaseCommandResult = server.executeNextCommand(reader.readCommand()).get();

                    writer.write(databaseCommandResult.serialize());
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Client socket threw IO Exception " + e.getMessage());
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.out.println("Got interrupted exception " + e.getMessage()); // todo sukhoa bad
            } catch (ExecutionException e) {
                e.printStackTrace();
                System.out.println("Got execution exception " + e.getMessage()); // todo sukhoa bad
            } catch (Throwable t) {
                t.printStackTrace();
                System.out.println("Unexpected exception" + t);
            } finally {
                close();
            }
        }

        /**
         * Закрывает клиентский сокет
         */
        @Override
        public void close() {
            try {
                client.close();
                System.out.println("Client has been disconnected: " + client.getRemoteSocketAddress());
            } catch (IOException e) {
                e.printStackTrace(); // todo sukhoa mmmmm
            }
        }
    }
}