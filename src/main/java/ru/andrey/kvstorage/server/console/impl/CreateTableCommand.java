package ru.andrey.kvstorage.server.console.impl;

import lombok.extern.slf4j.Slf4j;
import ru.andrey.kvstorage.server.console.DatabaseCommand;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.logic.Database;

import java.util.List;
import java.util.Optional;

@Slf4j
public class CreateTableCommand implements DatabaseCommand {
    private static final int MIN_NUMBER_OF_ARGS = 3;
    private static final int DATABASE_NAME_INDEX = 1;
    private static final int TABLE_NAME_INDEX = 2;

    private final ExecutionEnvironment env;
    private final String databaseName;
    private final String tableName;

    public CreateTableCommand(ExecutionEnvironment env, List<String> commandArgs) {
        if (commandArgs.size() < MIN_NUMBER_OF_ARGS) {
            log.error("Not enough arguments {} to create CreateTableCommand", commandArgs.size());
            throw new IllegalArgumentException("Not enough args");
        }
        this.databaseName = commandArgs.get(DATABASE_NAME_INDEX);
        this.tableName = commandArgs.get(TABLE_NAME_INDEX);
        this.env = env;
    }

    @Override
    public DatabaseCommandResult execute() throws DatabaseException {
        Optional<Database> database = env.getDatabase(databaseName);
        if (database.isEmpty()) {
            log.error("No such database {}", databaseName);
            throw new DatabaseException("No such database: " + databaseName);
        }
        database.get().createTableIfNotExists(tableName);
        log.info("Table {} successful created", tableName);
        return DatabaseCommandResult.success("Created table: " + tableName);
    }
}
