package ru.andrey.kvstorage.server.console.impl;

import lombok.extern.slf4j.Slf4j;
import ru.andrey.kvstorage.server.console.DatabaseCommand;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.logic.DatabaseFactory;

import java.util.List;

@Slf4j
public class CreateDatabaseCommand implements DatabaseCommand {
    private static final int MIN_NUMBER_OF_ARGS = 2;
    private static final int DATABASE_NAME_INDEX = 1;

    private final ExecutionEnvironment env;
    private final DatabaseFactory databaseFactory;
    private final String databaseName;

    public CreateDatabaseCommand(ExecutionEnvironment env, DatabaseFactory factory, List<String> args) {
        if (args.size() < MIN_NUMBER_OF_ARGS) {
            log.error("Not enough arguments {} to create CreateDatabaseCommand", args.size());
            throw new IllegalArgumentException("Not enough args");
        }
        this.env = env;
        this.databaseFactory = factory;
        this.databaseName = args.get(DATABASE_NAME_INDEX);
    }

    @Override
    public DatabaseCommandResult execute() throws DatabaseException {
        env.addDatabase(databaseFactory.createNonExistent(databaseName, env.getWorkingPath()));
        log.info("Database {} successful created", databaseName);
        return DatabaseCommandResult.success("Database: " + databaseName + "created");
    }
}
