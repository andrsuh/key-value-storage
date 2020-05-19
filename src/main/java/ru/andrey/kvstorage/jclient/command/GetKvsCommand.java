package ru.andrey.kvstorage.jclient.command;

import ru.andrey.kvstorage.resp.object.RespArray;
import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.resp.object.RespSimpleString;

public class GetKvsCommand implements KvsCommand {

    private static final String COMMAND_NAME = "READ_KEY";

    private final String databaseName;
    private final String tableName;
    private final String key;

    public GetKvsCommand(String databaseName, String tableName, String key) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.key = key;
    }

    @Override
    public RespObject serialize() {
        return new RespArray(
            new RespSimpleString(COMMAND_NAME),
            new RespSimpleString(databaseName),
            new RespSimpleString(tableName),
            new RespSimpleString(key)
        );
    }
}
