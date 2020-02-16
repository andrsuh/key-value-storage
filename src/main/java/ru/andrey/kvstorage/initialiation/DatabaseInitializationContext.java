package ru.andrey.kvstorage.initialiation;

import ru.andrey.kvstorage.logic.Table;

import java.nio.file.Path;
import java.util.Map;

public interface DatabaseInitializationContext {
    String getDbName();

    void setDbName(String dbName);

    Path getDatabasePath();

    void setDatabasePath(Path databasePath);

    Map<String, Table> getTables();

    void setTables(Map<String, Table> tables);
}

