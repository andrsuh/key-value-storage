package ru.andrey.kvstorage.server.console.impl;

import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.server.console.DatabaseCommand;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.exceptions.DatabaseException;
import ru.andrey.kvstorage.server.logic.Database;

import java.util.List;

import static ru.andrey.kvstorage.server.console.DatabaseCommandArgPositions.*;

/**
 * Команда для создания удаления значения по ключу
 */
public class DeleteKeyCommand implements DatabaseCommand {

    private final ExecutionEnvironment env;
    private final String databaseName;
    private final String tableName;
    private final String key;

    public DeleteKeyCommand(ExecutionEnvironment env, List<RespObject> args) {
        if (args.size() < KEY.getPositionIndex()) {
            throw new IllegalArgumentException("Not enough args");
        }
        this.env = env;
        this.databaseName = args.get(DATABASE_NAME.getPositionIndex()).asString();
        this.tableName = args.get(TABLE_NAME.getPositionIndex()).asString();
        this.key = args.get(KEY.getPositionIndex()).asString();
    }

    /**
     * @return удаленное значение. Например, "previous"
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

        byte[] prevValue; // todo sukhoa array to string
        try {
            prevValue = database.read(tableName, key)
                    .orElseThrow(() -> new DatabaseException("Unable to delete key \"" + key + "\". Key does not exist."));
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e);
        }

        try {
            database.delete(tableName, key);
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e);
        }
        return DatabaseCommandResult.success(prevValue);
    }
}
