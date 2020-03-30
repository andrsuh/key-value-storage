package ru.andrey.kvstorage.console.impl;

import ru.andrey.kvstorage.console.DatabaseCommand;
import ru.andrey.kvstorage.console.DatabaseCommandResult;
import ru.andrey.kvstorage.console.ExecutionEnvironment;
import ru.andrey.kvstorage.exception.DatabaseException;
import ru.andrey.kvstorage.logic.Database;

import java.util.Optional;

public class UpdateKeyCommand implements DatabaseCommand {
    private final ExecutionEnvironment env;
    private final String databaseName;
    private final String tableName;
    private final String key;
    private final String value;

    public UpdateKeyCommand(ExecutionEnvironment env, String... args) {
        if (args.length < 5) {
            throw new IllegalArgumentException("Not enough args");
        }

        this.databaseName = args[1];
        this.tableName = args[2];
        this.key = args[3];
        this.value = args[4];
        this.env = env;
    }

    @Override
    public DatabaseCommandResult execute() throws DatabaseException {
        Optional<Database> database = env.getDatabase(databaseName);
        if (database.isEmpty()) {
            throw new DatabaseException("No such database: " + databaseName);
        }
        database.get().write(tableName, key, value);
        return DatabaseCommandResult.success("Updated table: " + tableName + ", key: " + key);
    }
}
