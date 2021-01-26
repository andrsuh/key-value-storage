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
public class GetKeyCommand implements DatabaseCommand {
    private static final int MIN_NUMBER_OF_ARGS = 4;
    private static final int DATABASE_NAME_INDEX = 1;
    private static final int TABLE_NAME_INDEX = 2;
    private static final int KEY_INDEX = 3;

    private final ExecutionEnvironment env;
    private final String databaseName;
    private final String tableName;
    private final String key;

    public GetKeyCommand(ExecutionEnvironment env, List<String> args) {
        if (args.size() < MIN_NUMBER_OF_ARGS) {
            log.error("Not enough arguments {} to create GetKeyCommand", args.size());
            throw new IllegalArgumentException("Not enough args");
        }
        this.env = env;
        this.databaseName = args.get(DATABASE_NAME_INDEX);
        this.tableName = args.get(TABLE_NAME_INDEX);
        this.key = args.get(KEY_INDEX);
    }

    @Override
    public DatabaseCommandResult execute() throws DatabaseException {
        Database database = env.getDatabase(databaseName)
                .orElseThrow(() -> new DatabaseException("No such database: " + databaseName));
        Optional<String> result = database.read(tableName, key);
        return result
                .map(DatabaseCommandResult::success)
                //TODO: add specification for null value
                .orElseGet(() -> DatabaseCommandResult.success("null"));
    }
}
