package ru.andrey.kvstorage.server.console.impl;

import ru.andrey.kvstorage.server.console.DatabaseCommand;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.logic.Database;

import java.util.List;
import java.util.Optional;

public class CreateTableCommand implements DatabaseCommand {

    private final ExecutionEnvironment env;
    private final String databaseName;
    private final String tableName;

    public CreateTableCommand(ExecutionEnvironment env, List<String> commandArgs) {
        if (commandArgs.size() < 3) {
            throw new IllegalArgumentException("Not enough args");
        }
        this.databaseName = commandArgs.get(1);
        this.tableName = commandArgs.get(2);
        this.env = env;
    }

    @Override
    public DatabaseCommandResult execute() throws DatabaseException {
        Optional<Database> database = env.getDatabase(databaseName);
        if (database.isEmpty()) {
            throw new DatabaseException("No such database: " + databaseName);
        }
        database.get().createTableIfNotExists(tableName);
        return DatabaseCommandResult.success("Created table: " + tableName);
    }
}
