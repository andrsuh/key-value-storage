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

public class GetKeyCommand implements DatabaseCommand {

    private final ExecutionEnvironment env;
    private final String databaseName;
    private final String tableName;
    private final String key;

    public GetKeyCommand(ExecutionEnvironment env, List<RespObject> args) {
        if (args.size() < 4) {
            throw new IllegalArgumentException("Not enough args");
        }
        this.env = env;
        this.databaseName = args.get(DATABASE_NAME.getPositionIndex()).asString();
        this.tableName = args.get(TABLE_NAME.getPositionIndex()).asString();
        this.key = args.get(KEY.getPositionIndex()).asString();
    }

    /**
     * @return текущее значение. Например "value'
     */
    @Override
    public DatabaseCommandResult execute() throws DatabaseException {
        Database database = env.getDatabase(databaseName)
                .orElseThrow(() -> new DatabaseException("No such database: " + databaseName));
        Optional<byte[]> result = database.read(tableName, key);
        return result
                .map(DatabaseCommandResult::success)
                .orElseGet(() -> DatabaseCommandResult.success(null));
    }
}
