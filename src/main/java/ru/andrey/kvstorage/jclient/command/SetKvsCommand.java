package ru.andrey.kvstorage.jclient.command;

import ru.andrey.kvstorage.resp.object.RespArray;
import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.resp.object.RespSimpleString;

public class SetKvsCommand implements KvsCommand {

    private static final String COMMAND_NAME = "UPDATE_KEY";

    private final String databaseName;
    private final String tableName;
    private final String key;
    private final String value;

    public SetKvsCommand(String databaseName, String tableName, String key, String value) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.key = key;
        this.value = value;
    }

    @Override
    public RespObject serialize() {
        return new RespArray(
            new RespSimpleString(COMMAND_NAME),
            new RespSimpleString(databaseName),
            new RespSimpleString(tableName),
            new RespSimpleString(key),
            new RespSimpleString(value)
        );
    }
}
