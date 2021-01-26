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
public class SetKeyCommand implements DatabaseCommand {
    private static final int MIN_NUMBER_OF_ARGS = 5;
    private static final int DATABASE_NAME_INDEX = 1;
    private static final int TABLE_NAME_INDEX = 2;
    private static final int KEY_INDEX = 3;
    private static final int VALUE_INDEX = 4;

    private final ExecutionEnvironment env;
    private final String databaseName;
    private final String tableName;
    private final String key;
    private final String value;

    public SetKeyCommand(ExecutionEnvironment env, List<String> args) {
        if (args.size() < MIN_NUMBER_OF_ARGS) {
            log.error("Not enough arguments {} to create SetKeyCommand", args.size());
            throw new IllegalArgumentException("Not enough args");
        }
        this.env = env;
        this.databaseName = args.get(DATABASE_NAME_INDEX);
        this.tableName = args.get(TABLE_NAME_INDEX);
        this.key = args.get(KEY_INDEX);
        this.value = args.get(VALUE_INDEX);
    }

    @Override
    public DatabaseCommandResult execute() throws DatabaseException {
        Database database = env.getDatabase(databaseName)
                .orElseThrow(() -> new DatabaseException("No such database: " + databaseName));

        //TODO: add specification for null value
        String prevValue = database.read(tableName, key).orElse("null");
        database.write(tableName, key, value);
        return DatabaseCommandResult.success(prevValue);
    }
}
