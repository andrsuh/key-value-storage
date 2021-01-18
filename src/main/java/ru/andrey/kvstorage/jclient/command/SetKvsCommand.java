package ru.andrey.kvstorage.jclient.command;

import ru.andrey.kvstorage.resp.object.RespArray;
import ru.andrey.kvstorage.resp.object.RespBulkString;
import ru.andrey.kvstorage.resp.object.RespCommandId;

import java.nio.charset.StandardCharsets;

public class SetKvsCommand implements KvsCommand {

    private static final String COMMAND_NAME = "UPDATE_KEY";

    private final String databaseName;
    private final String tableName;
    private final String key;
    private final String value;
    private final RespCommandId commandId = new RespCommandId();

    public SetKvsCommand(String databaseName, String tableName, String key, String value) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.key = key;
        this.value = value;
    }

    @Override
    public RespArray serialize() {
        return new RespArray(
                commandId,
                new RespBulkString(COMMAND_NAME.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(databaseName.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(tableName.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(key.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(value.getBytes(StandardCharsets.UTF_8))
        );
    }

    @Override
    public int getCommandId() {
        return commandId.commandId;
    }
}
