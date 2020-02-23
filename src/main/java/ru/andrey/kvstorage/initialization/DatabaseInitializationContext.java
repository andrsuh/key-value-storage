package ru.andrey.kvstorage.initialization;

import ru.andrey.kvstorage.logic.Table;

import java.nio.file.Path;
import java.util.Map;

public interface DatabaseInitializationContext {
    String getDbName();

    Path getDatabasePath();

    Map<String, Table> getTables();

    void addTable(Table table);
}

