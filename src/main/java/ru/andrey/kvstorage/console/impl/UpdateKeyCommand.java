package ru.andrey.kvstorage.console.impl;

import ru.andrey.kvstorage.console.DatabaseCommand;
import ru.andrey.kvstorage.console.DatabaseCommandResult;
import ru.andrey.kvstorage.console.ExecutionEnvironment;
import ru.andrey.kvstorage.exception.DatabaseException;
import ru.andrey.kvstorage.logic.Database;

import java.util.Arrays;
import java.util.Optional;

public class UpdateKeyCommand implements DatabaseCommand {
    private final ExecutionEnvironment env;
    private final String databaseName;
    private final String tableName;
    private final String key;
    private final String value;

    public UpdateKeyCommand(ExecutionEnvironment env, String... args) {
        Object[] arguments = Arrays.stream(args)
                .skip(env.currentDatabase().isEmpty() ? 1 : 2)
                .map(Object::toString)
                .toArray();

        //  if (args.length < 1) todo sukhoa
        this.databaseName = env.currentDatabase().map(Database::getName).orElse((String) arguments[0]);
        this.tableName = (String) arguments[1];
        this.key = (String) arguments[2];
        this.value = (String) arguments[3];
        this.env = env;
    }

    @Override
    public DatabaseCommandResult execute() throws DatabaseException {
        // if .... todo sukhoa
        Optional<Database> database = env.getDatabase(databaseName);
        if (database.isPresent()) {
            database.get().write(tableName, key, value);
            return DatabaseCommandResult.success("Updated table: " + tableName + ", key: " + key);
        }
        throw new DatabaseException("No such database: " + databaseName);
    }
}
