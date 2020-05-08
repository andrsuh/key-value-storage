package ru.andrey.kvstorage.jclient.client;

import ru.andrey.kvstorage.jclient.DatabaseResponseParser;
import ru.andrey.kvstorage.jclient.command.GetKvsCommand;
import ru.andrey.kvstorage.jclient.connection.KvsConnection;
import ru.andrey.kvstorage.jclient.exception.KvsConnectionException;

import java.util.function.Supplier;

// It is not supposed to be thread-safe
public class SimpleKvsClient implements KvsClient {
    private final Supplier<KvsConnection> connectionSupplier;
    private final DatabaseResponseParser responseParser;
    private final String databaseName; // todo sukhoa make SimpleKvsClient get something like "ConnectionSettings" or "ConnectionConfigurations" class

    private KvsConnection connection;

    public SimpleKvsClient(
            String databaseName,
            Supplier<KvsConnection> connectionSupplier, // todo sukhoa Connection factory?
            DatabaseResponseParser responseParser
    ) {
        this.connectionSupplier = connectionSupplier;
        this.databaseName = databaseName;
        this.connection = connectionSupplier.get();
        this.responseParser = responseParser;
    }

    @Override
    public String get(String tableName, String key) {
        if (connection == null) {
            connection = connectionSupplier.get();
        }

        try {
            return responseParser.parseResponse(
                    connection.send(new GetKvsCommand(databaseName, tableName, key).toApiBytesRepresentation()));
        } catch (Exception e) {
            // IO exception in future
            try {
                connection.close();
            } catch (KvsConnectionException ex) {
                throw new IllegalStateException("Cannot close the connection", ex);
            }
            throw new IllegalStateException("Connection io exception", e);
        }
    }

    @Override
    public String set(String key, String value) {
        throw new UnsupportedOperationException();
    }
}
