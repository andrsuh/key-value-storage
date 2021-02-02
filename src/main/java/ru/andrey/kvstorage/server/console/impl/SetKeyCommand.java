package ru.andrey.kvstorage.server.console.impl;

import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.server.console.DatabaseCommand;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.logic.Database;

import java.util.List;
import java.util.Optional;

import static ru.andrey.kvstorage.server.console.DatabaseCommandArgPositions.*;

public class SetKeyCommand implements DatabaseCommand {

    private final ExecutionEnvironment env;
    private final String databaseName;
    private final String tableName;
    private final String key;
    private final byte[] value;

    public SetKeyCommand(ExecutionEnvironment env, List<RespObject> args) {
        if (args.size() < 5) {
            throw new IllegalArgumentException("Not enough args");
        }
        this.env = env;
        this.databaseName = args.get(DATABASE_NAME.getPositionIndex()).asString();
        this.tableName = args.get(TABLE_NAME.getPositionIndex()).asString();
        this.key = args.get(KEY.getPositionIndex()).asString();
        this.value = args.get(VALUE.getPositionIndex()).getPayloadBytes();
    }

    @Override
    public DatabaseCommandResult execute() throws DatabaseException {
        Database database = env.getDatabase(databaseName)
                .orElseThrow(() -> new DatabaseException("No such database: " + databaseName));

        Optional<byte[]> prevValue = database.read(tableName, key);
        database.write(tableName, key, value);
        return DatabaseCommandResult.success(prevValue.orElse(null));
    }
}
