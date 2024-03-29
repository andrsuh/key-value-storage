package ru.andrey.kvstorage.jclient.command;

import ru.andrey.kvstorage.resp.object.RespArray;
import ru.andrey.kvstorage.resp.object.RespBulkString;
import ru.andrey.kvstorage.resp.object.RespCommandId;

import java.nio.charset.StandardCharsets;

/**
 * Команда для создания таблицы
 */
public class CreateTableKvsCommand implements KvsCommand {
    private static final String COMMAND_NAME = "CREATE_TABLE";

    private final String databaseName;
    private final String tableName;
    private final RespCommandId commandId = new RespCommandId(idGen.getAndIncrement());

    public CreateTableKvsCommand(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    @Override
    public RespArray serialize() {
        return new RespArray(
                commandId,
                new RespBulkString(COMMAND_NAME.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(databaseName.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(tableName.getBytes(StandardCharsets.UTF_8))
        );
    }

    @Override
    public int getCommandId() {
        return commandId.commandId;
    }
}
