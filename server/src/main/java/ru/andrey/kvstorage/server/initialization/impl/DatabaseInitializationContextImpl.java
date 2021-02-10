package ru.andrey.kvstorage.server.initialization.impl;

import lombok.Getter;
import ru.andrey.kvstorage.server.initialization.DatabaseInitializationContext;
import ru.andrey.kvstorage.server.logic.Table;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

@Getter
public class DatabaseInitializationContextImpl implements DatabaseInitializationContext {
    private final String dbName;
    private final Path databasePath;
    private final Map<String, Table> tables = new HashMap<>(16);

    public DatabaseInitializationContextImpl(String dbName, Path databaseRoot) {
        this.dbName = dbName;
        this.databasePath = databaseRoot.resolve(dbName);
    }

    @Override
    public void addTable(Table table) {
        tables.put(table.getName(), table); // todo sukhoa check if already exists
    }
}
