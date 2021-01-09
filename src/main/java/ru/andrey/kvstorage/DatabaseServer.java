package ru.andrey.kvstorage;

import org.apache.commons.lang3.StringUtils;
import ru.andrey.kvstorage.resp.RespReader;
import ru.andrey.kvstorage.resp.RespWriter;
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class DatabaseServer {

    private static ExecutorService executor = Executors.newSingleThreadExecutor();

    private final ServerSocket serverSocket;
    private final ExecutionEnvironment env;

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

        // databaseServer.executeNextCommand("UPDATE_KEY test_3 Post 1 {\"title\":\"post\",\"user\":\"andrey\",\"content\":\"bla\"}");

        while (true) {
            executor.submit(new ClientTask(databaseServer.serverSocket.accept(), databaseServer));
        }
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

                while (!client.isClosed()) {
                    if (!reader.hasNextCommand()) continue;

                    writer.write(server.executeNextCommand(reader.readCommand()).serialize());
                }
            } catch (IOException e) {
                System.out.println("Client socket threw IO Exception " + e.getMessage());
            } finally {
                close();
            }
        }

        @Override
        public void close() {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
