package ru.andrey.kvstorage.server.console.impl;

import ru.andrey.kvstorage.server.console.DatabaseCommand;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.logic.Database;

import java.util.List;
import java.util.Optional;

public class SetKeyCommand implements DatabaseCommand {

    private final ExecutionEnvironment env;
    private final String databaseName;
    private final String tableName;
    private final String key;
    private final String value;

    public SetKeyCommand(ExecutionEnvironment env, List<String> args) {
        if (args.size() < 5) {
            throw new IllegalArgumentException("Not enough args");
        }
        this.env = env;
        this.databaseName = args.get(1);
        this.tableName = args.get(2);
        this.key = args.get(3);
        this.value = args.get(4);
    }

    @Override
    public DatabaseCommandResult execute() throws DatabaseException {
        Database database = env.getDatabase(databaseName)
                .orElseThrow(() -> new DatabaseException("No such database: " + databaseName));

        Optional<String> prevValue = database.read(tableName, key);
        database.write(tableName, key, value);
        return DatabaseCommandResult.success(prevValue.orElse(null));
    }
}
