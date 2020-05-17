package ru.andrey.kvstorage.jclient.command;

import java.util.List;

public class GetKvsCommand implements KvsCommand {
    private static final String COMMAND_NAME = "READ_KEY"; // todo is this ok?
    private final String tableName;
    private final String databaseName;
    private final String key;

    public GetKvsCommand(String databaseName, String tableName, String key) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.key = key;
    }

    @Override
    public List<String> asList() {
        return List.of(COMMAND_NAME, databaseName, tableName, key, "\r");
    }
}
