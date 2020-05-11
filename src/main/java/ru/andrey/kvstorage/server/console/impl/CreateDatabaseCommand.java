package ru.andrey.kvstorage.server.console.impl;

import ru.andrey.kvstorage.server.console.DatabaseCommand;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.logic.DatabaseFactory;

import java.nio.file.Path;

public class CreateDatabaseCommand implements DatabaseCommand {
    private final ExecutionEnvironment env;
    private final String databaseName;
    private final DatabaseFactory databaseFactory;

    public CreateDatabaseCommand(ExecutionEnvironment env, DatabaseFactory factory, String... args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("Not enough args");
        }
        this.databaseFactory = factory;
        this.databaseName = args[1];
        this.env = env;
    }

    @Override
    public DatabaseCommandResult execute() throws DatabaseException {
        env.addDatabase(databaseFactory.createNonExistent(databaseName, Path.of(""))); // todo sukhoa fix path
        return DatabaseCommandResult.success("Database: " + databaseName + "created");
    }
}
