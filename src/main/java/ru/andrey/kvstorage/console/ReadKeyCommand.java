package ru.andrey.kvstorage.console;

import ru.andrey.kvstorage.exception.DatabaseException;
import ru.andrey.kvstorage.logic.Database;

import java.util.Arrays;
import java.util.Optional;

public class ReadKeyCommand implements DatabaseCommand {
    private final ExecutionEnvironment env;
    private final String databaseName;
    private final String tableName;
    private final String key;

    public ReadKeyCommand(ExecutionEnvironment env, String... args) {
        Object[] arguments = Arrays.stream(args)
                .skip(env.currentDatabase().isEmpty() ? 1 : 2)
                .map(Object::toString)
                .toArray();

        //  if (args.length < 1) todo sukhoa
        this.databaseName = env.currentDatabase().map(Database::getName).orElse((String) arguments[0]);
        this.tableName = (String) arguments[1];
        this.key = (String) arguments[2];
        this.env = env;
    }

    @Override
    public DatabaseCommandResult execute() throws DatabaseException {
        // if .... todo sukhoa
        Optional<Database> database = env.getDatabase(databaseName);
        if (database.isPresent()) {
            String result = database.get().read(tableName, key);
            return new DatabaseCommandResultImpl("Read key: " + key, result);
        }
        throw new DatabaseException("No such database: " + databaseName);
    }
}
