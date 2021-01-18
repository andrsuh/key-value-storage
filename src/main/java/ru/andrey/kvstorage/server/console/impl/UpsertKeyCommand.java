package ru.andrey.kvstorage.server.console.impl;

import ru.andrey.kvstorage.server.console.DatabaseCommand;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.logic.Database;

import java.util.List;
import java.util.Optional;

public class UpsertKeyCommand implements DatabaseCommand {

    private final ExecutionEnvironment env;
    private final String databaseName;
    private final String tableName;
    private final String key;
    private final String value;

    public UpsertKeyCommand(ExecutionEnvironment env, List<String> args) {
        if (args.size() < 5) {
            throw new IllegalArgumentException("Not enough args");
        }
        this.env = env;
        this.databaseName = args.get(1);
        this.tableName = args.get(2);
        this.key = args.get(3);
        this.value = args.get(4);
    }

    @Override
    public DatabaseCommandResult execute() throws DatabaseException {
        Optional<Database> database = env.getDatabase(databaseName);
        if (database.isEmpty()) {
            throw new DatabaseException("No such database: " + databaseName);
        }
        Optional<String> prevValue = database.get().read(tableName, key);
        if (prevValue.isEmpty())
            throw new IllegalStateException("Read nonexistent key");

        database.get().write(tableName, key, value);
        return DatabaseCommandResult.success(value);
    }
}
