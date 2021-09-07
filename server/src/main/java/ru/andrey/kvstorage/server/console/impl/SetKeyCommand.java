package ru.andrey.kvstorage.server.console.impl;

import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.server.console.DatabaseCommand;
import ru.andrey.kvstorage.server.console.DatabaseCommandArgPositions;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.exceptions.DatabaseException;
import ru.andrey.kvstorage.server.logic.Database;

import java.util.List;
import java.util.Optional;


/**
 * Команда для создания записи значения
 */
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
        this.databaseName = args.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
        this.tableName = args.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
        this.key = args.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
        this.value = args.get(DatabaseCommandArgPositions.VALUE.getPositionIndex()).asString().getBytes();
    }

    /**
     * @return предыдущее значение. Например, "previous" или null, если такого не было
     */
    @Override
    public DatabaseCommandResult execute() {
        Database database;
        try {
            database = env.getDatabase(databaseName)
                    .orElseThrow(() -> new RuntimeException("No such database: " + databaseName));
        } catch (Exception e) {
            return DatabaseCommandResult.error(e);
        }

        Optional<byte[]> prevValue;
        try {
            prevValue = database.read(tableName, key);
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e);
        }
        try {
            database.write(tableName, key, value);
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e);
        }
        return DatabaseCommandResult.success(prevValue.orElse(null));
    }
}
