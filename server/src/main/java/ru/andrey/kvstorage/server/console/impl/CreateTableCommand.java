package ru.andrey.kvstorage.server.console.impl;

import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.server.console.DatabaseCommand;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.logic.Database;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static ru.andrey.kvstorage.server.console.DatabaseCommandArgPositions.DATABASE_NAME;
import static ru.andrey.kvstorage.server.console.DatabaseCommandArgPositions.TABLE_NAME;

public class CreateTableCommand implements DatabaseCommand {

    private final ExecutionEnvironment env;
    private final String databaseName;
    private final String tableName;

    public CreateTableCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        if (commandArgs.size() < 3) {
            throw new IllegalArgumentException("Not enough args");
        }
        this.databaseName = commandArgs.get(DATABASE_NAME.getPositionIndex()).asString();
        this.tableName = commandArgs.get(TABLE_NAME.getPositionIndex()).asString();
        this.env = env;
    }

    /**
     * @return сообщение о том, что заданная таблица была создана. Например, "Table table1 in database db1 created"
     */
    @Override
    public DatabaseCommandResult execute() throws DatabaseException {
        Optional<Database> database = env.getDatabase(databaseName);
        if (database.isEmpty()) {
            throw new DatabaseException("No such database: " + databaseName);
        }
        database.get().createTableIfNotExists(tableName);
        return DatabaseCommandResult.success(("Created table: " + tableName).getBytes(StandardCharsets.UTF_8));
    }
}
