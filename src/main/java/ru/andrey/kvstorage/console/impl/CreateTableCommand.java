package ru.andrey.kvstorage.console.impl;

import ru.andrey.kvstorage.console.DatabaseCommand;
import ru.andrey.kvstorage.console.DatabaseCommandResult;
import ru.andrey.kvstorage.console.ExecutionEnvironment;
import ru.andrey.kvstorage.exception.DatabaseException;
import ru.andrey.kvstorage.logic.Database;

import java.util.Arrays;
import java.util.Optional;

public class CreateTableCommand implements DatabaseCommand {
    private final ExecutionEnvironment env;
    private final String databaseName;
    private final String tableName;

    public CreateTableCommand(ExecutionEnvironment env, String... args) {
        Object[] arguments = Arrays.stream(args)
                .skip(env.currentDatabase().isEmpty() ? 1 : 2)
                .map(Object::toString)
                .toArray();

        //  if (args.length < 1) todo sukhoa
        this.databaseName = env.currentDatabase().map(Database::getName).orElse((String) arguments[0]);
        this.tableName = (String) arguments[1];
        this.env = env;
    }

    @Override
    public DatabaseCommandResult execute() throws DatabaseException {
        // if .... todo sukhoa
        Optional<Database> database = env.getDatabase(databaseName);
        if (database.isPresent()) {
            database.get().createTableIfNotExists(tableName);
            return new DatabaseCommandResultImpl("Created table: " + tableName, null);
        }
        throw new DatabaseException("No such database: " + databaseName);
    }
}
