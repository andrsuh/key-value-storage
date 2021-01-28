package ru.andrey.kvstorage.server.console.impl;

import ru.andrey.kvstorage.server.console.DatabaseCommand;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.logic.Database;

import java.util.List;

public class DeleteKeyCommand implements DatabaseCommand {
    private final ExecutionEnvironment env;
    private final String databaseName;
    private final String tableName;
    private final String key;

    public DeleteKeyCommand(ExecutionEnvironment env, List<String> args) {
        if (args.size() < 4) {
            throw new IllegalArgumentException("Not enough args");
        }
        this.env = env;
        this.databaseName = args.get(1);
        this.tableName = args.get(2);
        this.key = args.get(3);
    }

    @Override
    public DatabaseCommandResult execute() throws DatabaseException {
        Database database = env.getDatabase(databaseName)
                .orElseThrow(() -> new DatabaseException("No such database: " + databaseName));

        String prevValue = database.read(tableName, key).orElseThrow(() ->
                new DatabaseException("Unable to delete key \"" + key + "\". Key does not exist."));

        database.delete(tableName, key);
        return DatabaseCommandResult.success(prevValue);
    }
}
