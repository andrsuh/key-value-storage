package ru.andrey.kvstorage.console;

import ru.andrey.kvstorage.exception.DatabaseException;
import ru.andrey.kvstorage.logic.Database;
import ru.andrey.kvstorage.logic.impl.DatabaseImpl;

import java.nio.file.Path;

public class CreateDatabaseCommand implements DatabaseCommand {
    private final ExecutionEnvironment env;
    private final String databaseName;

    public CreateDatabaseCommand(ExecutionEnvironment env, String... args) {
        //  if (args.length < 1) todo sukhoa
        this.databaseName = env.currentDatabase().map(Database::getName).orElse(args[1]);
        this.env = env;
    }

    @Override
    public DatabaseCommandResult execute() throws DatabaseException {
        // if .... todo sukhoa
//        Optional<Database> database = env.getDatabase(databaseName);
//        if (database.isPresent()) {
//            database.get().
//        }
        env.addDatabase(DatabaseImpl.create(databaseName, Path.of(""))); // todo sukhoa fix path
        return new DatabaseCommandResultImpl("Database: " + databaseName + "created", null);
    }
}
