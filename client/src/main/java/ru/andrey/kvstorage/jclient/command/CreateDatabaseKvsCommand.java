package ru.andrey.kvstorage.jclient.command;

import ru.andrey.kvstorage.resp.object.RespArray;
import ru.andrey.kvstorage.resp.object.RespBulkString;
import ru.andrey.kvstorage.resp.object.RespCommandId;

import java.nio.charset.StandardCharsets;

/**
 * Команда для создания бд
 */
public class CreateDatabaseKvsCommand implements KvsCommand {
    private static final String COMMAND_NAME = "CREATE_DATABASE";

    private final String databaseName;
    private final RespCommandId commandId = new RespCommandId(idGen.getAndIncrement());

    public CreateDatabaseKvsCommand(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public RespArray serialize() {
        return new RespArray(
                commandId,
                new RespBulkString(COMMAND_NAME.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(databaseName.getBytes(StandardCharsets.UTF_8))
        );
    }

    @Override
    public int getCommandId() {
        return commandId.commandId;
    }
}
