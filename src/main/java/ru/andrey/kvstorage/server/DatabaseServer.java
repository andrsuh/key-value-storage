package ru.andrey.kvstorage.server;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import ru.andrey.kvstorage.resp.object.RespArray;
import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.server.console.DatabaseCommand;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;
import ru.andrey.kvstorage.server.console.DatabaseCommands;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseServerInitializer;
import ru.andrey.kvstorage.server.initialization.impl.InitializationContextImpl;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static ru.andrey.kvstorage.server.console.DatabaseCommandArgPositions.COMMAND_NAME;

@Slf4j
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

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(RespObject msg) {
        try {
            RespArray message = (RespArray) msg;
            log.debug("Server got client request: [ {} ]", message);

            List<RespObject> commandArgs = message.getObjects();
            DatabaseCommand command = DatabaseCommands
                    .valueOf(commandArgs.get(COMMAND_NAME.getPositionIndex()).asString())
                    .getCommand(env, commandArgs);

            return executeNextCommand(command);
        } catch (Exception e) {
            log.error(e.getMessage());
            return CompletableFuture.completedFuture(DatabaseCommandResult.error(e));
        }
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(DatabaseCommand command) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return command.execute();
            } catch (DatabaseException e) {
                log.error(e.getMessage());
                return DatabaseCommandResult.error(e);
            }
        }, dbCommandExecutor);
    }
}