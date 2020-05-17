package ru.andrey.kvstorage;

import ru.andrey.kvstorage.server.console.DatabaseCommandResult;
import ru.andrey.kvstorage.server.console.DatabaseCommands;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.console.impl.ExecutionEnvironmentImpl;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.initialization.Initializer;
import ru.andrey.kvstorage.server.initialization.impl.*;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseServer {

    private static ExecutorService executor = Executors.newSingleThreadExecutor();
    private final ExecutionEnvironment env;
    private ServerSocket serverSocket;

    public DatabaseServer(ExecutionEnvironment env, Initializer initializer) throws IOException, DatabaseException {
        this.serverSocket = new ServerSocket(4321);
        this.env = env;

        InitializationContextImpl initializationContext = InitializationContextImpl.builder()
                .executionEnvironment(env)
                .build();

        initializer.perform(initializationContext);
    }

    public static void main(String[] args) throws IOException, DatabaseException {

        Initializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));

        DatabaseServer databaseServer = new DatabaseServer(new ExecutionEnvironmentImpl(), initializer);

        while (true) {
            executor.submit(new ClientTask(databaseServer.serverSocket.accept(), databaseServer));
        }
    }

    public byte[] executeNextCommandAndGetApiBytes(byte[] commandText) {
        return executeNextCommand(new String(commandText)).toApiBytes();
    }

    public DatabaseCommandResult executeNextCommand(String commandText) {
        try {
            String[] commandInfo = commandText.split(" ");

            return DatabaseCommands.valueOf(commandInfo[0])
                    .getCommand(env, commandInfo)
                    .execute();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            String message = e.getMessage() != null
                    ? e.getMessage()
                    : Arrays.toString(e.getStackTrace());
            return DatabaseCommandResult.error(message);
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
        public void close() {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            try {
                var bufferedReader = new BufferedReader(new InputStreamReader(client.getInputStream()));
                var out = client.getOutputStream();

                while (!client.isClosed()) {
                    int b, i = 0;
                    byte[] res = new byte[1000]; // todo sukhoa remove magic constant

                    while ((b = bufferedReader.read()) != -1 && b != '\r') {
                        res[i++] = (byte) b;
                    }
                    if (i > 0) {
                        out.write(server.executeNextCommandAndGetApiBytes(Arrays.copyOfRange(res, 0, i)));
                        out.flush();
                    }
                }
            } catch (IOException e) {
                System.out.println("CLient socket threw IO Exception " + e.getMessage());
            } finally {
                close();
            }
        }
    }
}
