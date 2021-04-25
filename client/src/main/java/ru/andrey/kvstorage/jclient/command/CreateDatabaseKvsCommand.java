package ru.andrey.kvstorage.jclient.command;

import ru.andrey.kvstorage.resp.object.RespArray;
import ru.andrey.kvstorage.resp.object.RespBulkString;
import ru.andrey.kvstorage.resp.object.RespCommandId;
import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.server.console.DatabaseCommands;

import java.nio.charset.StandardCharsets;

public class CreateDatabaseKvsCommand  implements KvsCommand {
    private static final DatabaseCommands COMMAND_NAME = DatabaseCommands.CREATE_DATABASE;

    private final String databaseName;
    private final RespCommandId commandId = new RespCommandId(KvsCommand.idGen.getAndIncrement());

    public CreateDatabaseKvsCommand(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public RespObject serialize() {
        return new RespArray(
                commandId,
                new RespBulkString(COMMAND_NAME.toString().getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(databaseName.getBytes(StandardCharsets.UTF_8))
        );
    }

    @Override
    public int getCommandId() {
        return commandId.commandId;
    }
}
