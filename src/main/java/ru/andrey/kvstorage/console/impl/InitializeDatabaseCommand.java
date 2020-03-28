package ru.andrey.kvstorage.console.impl;

import ru.andrey.kvstorage.console.DatabaseCommand;
import ru.andrey.kvstorage.console.DatabaseCommandResult;
import ru.andrey.kvstorage.console.ExecutionEnvironment;
import ru.andrey.kvstorage.exception.DatabaseException;
import ru.andrey.kvstorage.initialization.Initializer;
import ru.andrey.kvstorage.initialization.impl.DatabaseInitializationContextImpl;
import ru.andrey.kvstorage.initialization.impl.InitializationContextImpl;

import java.nio.file.Path;

public class InitializeDatabaseCommand implements DatabaseCommand {
    private final ExecutionEnvironment env;
    private final String databaseName;
    private final Initializer initializer;

    public InitializeDatabaseCommand(ExecutionEnvironment env, Initializer initializer, String... args) {
        //  if (args.length < 1) todo sukhoa
        this.databaseName = args[1]; // todo sukhoa check AIOOB
        this.env = env;
        this.initializer = initializer;
    }

    @Override
    public DatabaseCommandResult execute() throws DatabaseException {
        // if .... todo sukhoa
//        Optional<Database> database = env.getDatabase(databaseName);
//        if (database.isPresent()) {
//            database.get().
//        }

        InitializationContextImpl initializationContext = InitializationContextImpl.builder()
                .executionEnvironment(env)
                .currentDatabaseContext(new DatabaseInitializationContextImpl(databaseName, Path.of(""))) // todo sukhoa fix path
                .build();

        initializer.perform(initializationContext);

        return DatabaseCommandResult.success("Database: " + databaseName + "initialized");
    }
}
