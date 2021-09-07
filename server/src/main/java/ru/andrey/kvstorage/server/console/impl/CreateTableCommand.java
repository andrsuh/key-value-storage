package ru.andrey.kvstorage.server.console.impl;

import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.server.console.DatabaseCommand;
import ru.andrey.kvstorage.server.console.DatabaseCommandArgPositions;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.exceptions.DatabaseException;
import ru.andrey.kvstorage.server.logic.Database;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Команда для создания базы таблицы
 */
public class CreateTableCommand implements DatabaseCommand {

    private final ExecutionEnvironment env;
    private final String databaseName;
    private final String tableName;

    public CreateTableCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        if (commandArgs.size() <= DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()) {
            throw new IllegalArgumentException("Not enough args");
        }
        this.databaseName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
        this.tableName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
        this.env = env;
    }

    /**
     * @return сообщение о том, что заданная таблица была создана. Например, "Table table1 in database db1 created"
     */
    @Override
    public DatabaseCommandResult execute() {
        Optional<Database> database = env.getDatabase(databaseName);
        if (database.isEmpty()) {
            return DatabaseCommandResult.error("No such database: " + databaseName);
        }
        try {
            database.get().createTableIfNotExists(tableName);
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e);
        }
        return DatabaseCommandResult.success(("Created table: " + tableName).getBytes(StandardCharsets.UTF_8));
    }
}
