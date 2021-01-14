package ru.andrey.kvstorage.jclient.client;

import ru.andrey.kvstorage.jclient.command.GetKvsCommand;
import ru.andrey.kvstorage.jclient.command.KvsCommand;
import ru.andrey.kvstorage.jclient.command.UpsertKvsCommand;
import ru.andrey.kvstorage.jclient.connection.KvsConnection;
import ru.andrey.kvstorage.jclient.exception.KvsConnectionException;
import ru.andrey.kvstorage.resp.object.RespObject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

// It is not supposed to be thread-safe
public class SimpleKvsClient implements KvsClient {
    private final Supplier<KvsConnection> connectionSupplier;
    private final String databaseName; // todo sukhoa make SimpleKvsClient get something like "ConnectionSettings" or "ConnectionConfigurations" class

    public static final Map<Integer, Object> requests = new ConcurrentHashMap<>();
    public static final Map<Integer, RespObject> responses = new ConcurrentHashMap<>();


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
    public String upsert(String tableName, String key, String value) {
        return executeCommand(new UpsertKvsCommand(databaseName, tableName, key, value));
    }

    private String executeCommand(KvsCommand command) {
        KvsConnection connection = connectionSupplier.get();

        requests.putIfAbsent(command.getCommandId(), command);

        synchronized (command) {
            try {
                connection.send(command.serialize());
                command.wait(); // todo sukhoa we can implement awaitility in temporary manner

                return handleResponse(responses.get(command.getCommandId()));
            } catch (Exception e) {
                // IO exception in future
                try {
                    connection.close(); // todo sukhoa schedule session rebind
                } catch (KvsConnectionException ex) {
                    throw new IllegalStateException("Cannot close the connection", ex);
                }
                throw new IllegalStateException("Connection io exception", e);
            } finally {
                requests.remove(command.getCommandId());
                responses.remove(command.getCommandId());
            }
        }
    }

    private String handleResponse(RespObject response) {
        if (response.isError()) {
            throw new RuntimeException(response.asString());
        }

        return response.asString();
    }
}
