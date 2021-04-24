package ru.andrey.kvstorage.jclient.client;

import ru.andrey.kvstorage.jclient.command.*;
import ru.andrey.kvstorage.jclient.connection.KvsConnection;
import ru.andrey.kvstorage.resp.object.RespObject;

import java.util.function.Supplier;

public class SimpleKvsClient implements KvsClient {
    private final Supplier<KvsConnection> connectionSupplier;
    private final String databaseName; // todo sukhoa make SimpleKvsClient get something like "ConnectionSettings" or "ConnectionConfigurations" class

    public SimpleKvsClient(
            String databaseName,
            Supplier<KvsConnection> connectionSupplier // todo sukhoa Connection factory?
    ) {
        this.connectionSupplier = connectionSupplier;
        this.databaseName = databaseName;
    }

    @Override
    public String get(String tableName, String key) {
        return executeCommand(new GetKvsCommand(databaseName, tableName, key));
    }

    @Override
    public String set(String tableName, String key, String value) {
        return executeCommand(new SetKvsCommand(databaseName, tableName, key, value));
    }

    @Override
    public String delete(String tableName, String key) {
        return executeCommand(new DeleteKvsCommand(databaseName, tableName, key));
    }

    @Override
    public String createDatabase(String databaseName) {
        return executeCommand(new CreateDatabaseKvsCommand(databaseName));
    }

    @Override
    public String executeCommand(String commandString) {
        return executeCommand(new StringKsvCommand(commandString));
    }

    private String executeCommand(KvsCommand command) {
        KvsConnection connection = connectionSupplier.get();

        try {
            RespObject serverResponse = connection.send(command.getCommandId(), command.serialize());

            if (serverResponse.isError()) {
                throw new RuntimeException(serverResponse.asString());
            }

            return serverResponse.asString();
        } catch (Exception e) {
            connection.close(); // todo sukhoa schedule session rebind
            throw new IllegalStateException("Connection io exception", e);
        }
    }
}
