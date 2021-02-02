package ru.andrey.kvstorage.server.console.impl;

import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.server.console.DatabaseCommand;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.logic.DatabaseFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static ru.andrey.kvstorage.server.console.DatabaseCommandArgPositions.DATABASE_NAME;

public class CreateDatabaseCommand implements DatabaseCommand {

    private final ExecutionEnvironment env;
    private final DatabaseFactory databaseFactory;
    private final String databaseName;

    public CreateDatabaseCommand(ExecutionEnvironment env, DatabaseFactory factory, List<RespObject> args) {
        if (args.size() < 2) {
            throw new IllegalArgumentException("Not enough args");
        }
        this.env = env;
        this.databaseFactory = factory;
        this.databaseName = args.get(DATABASE_NAME.getPositionIndex()).asString();
    }

    @Override
    public DatabaseCommandResult execute() throws DatabaseException {
        env.addDatabase(databaseFactory.createNonExistent(databaseName, env.getWorkingPath()));
        return DatabaseCommandResult.success(("Database: " + databaseName + "created").getBytes(StandardCharsets.UTF_8));
    }
}
