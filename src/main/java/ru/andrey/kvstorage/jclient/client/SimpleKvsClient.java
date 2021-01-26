package ru.andrey.kvstorage.jclient.client;

import lombok.extern.slf4j.Slf4j;
import ru.andrey.kvstorage.jclient.command.GetKvsCommand;
import ru.andrey.kvstorage.jclient.command.KvsCommand;
import ru.andrey.kvstorage.jclient.command.SetKvsCommand;
import ru.andrey.kvstorage.jclient.connection.KvsConnection;
import ru.andrey.kvstorage.jclient.exception.KvsConnectionException;
import ru.andrey.kvstorage.resp.object.RespObject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

// It is not supposed to be thread-safe
@Slf4j
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
        log.info("Getting {} from the {}", key, tableName);
        return executeCommand(new GetKvsCommand(databaseName, tableName, key));
    }

    @Override
    public String set(String tableName, String key, String value) {
        log.info("Setting {} : {} to the {}", key, value, tableName);
        return executeCommand(new SetKvsCommand(databaseName, tableName, key, value));
    }

    private String executeCommand(KvsCommand command) {
        log.debug("Executing command with id {}", command.getCommandId());
        KvsConnection connection = connectionSupplier.get();

        requests.putIfAbsent(command.getCommandId(), command);

        synchronized (command) {
            try {
                log.debug("Sending serialized command {} by the connection", command.getCommandId());
                connection.send(command.serialize());
                log.debug("Waiting for the  command {}", command.getCommandId());
                command.wait(); // todo sukhoa we can implement awaitility in temporary manner
                log.debug("Handling of the command {} execution response", command.getCommandId());
                return handleResponse(responses.get(command.getCommandId()));
            } catch (Exception e) {
                log.warn("Exception {} while executing command {}.", command.getCommandId(), e.getMessage());
                // IO exception in future
                try {
                    log.debug("Trying to close connection after command {} execution fail", command.getCommandId());
                    connection.close(); // todo sukhoa schedule session rebind
                } catch (KvsConnectionException ex) {
                    log.warn("Can't close connection {}", ex.getMessage());
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
            log.warn("Error while executing command {}", response.asString());
            throw new RuntimeException(response.asString());
        }

        return response.asString();
    }
}
