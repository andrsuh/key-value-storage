package ru.andrey.kvstorage.server;

import lombok.Getter;
import ru.andrey.kvstorage.resp.object.RespArray;
import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.server.console.*;
import ru.andrey.kvstorage.server.exceptions.DatabaseException;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseServerInitializer;
import ru.andrey.kvstorage.server.initialization.impl.InitializationContextImpl;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseServer {
    @Getter
    private final ExecutionEnvironment env;
    private final ExecutorService dbCommandExecutor = Executors.newSingleThreadExecutor();

    public DatabaseServer(ExecutionEnvironment env, DatabaseServerInitializer initializer) throws DatabaseException {
        this.env = env;

        InitializationContextImpl initializationContext = InitializationContextImpl.builder()
                .executionEnvironment(env)
                .build();

        initializer.perform(initializationContext);
    }

    public static DatabaseServer initialize(ExecutionEnvironment environment, DatabaseServerInitializer initializer) throws DatabaseException {
        return new DatabaseServer(environment, initializer);
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(RespArray message) {
        try {
            System.out.println("Server got client request: [ $message]");

            List<RespObject> commandArgs = message.getObjects();
            DatabaseCommand command = DatabaseCommands
                    .valueOf(commandArgs.get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex()).asString())
                    .getCommand(env, commandArgs);

            return executeNextCommand(command);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return CompletableFuture.completedFuture(DatabaseCommandResult.error(e));
        }
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(DatabaseCommand command) {
        return CompletableFuture.supplyAsync(command::execute, dbCommandExecutor);
    }
}