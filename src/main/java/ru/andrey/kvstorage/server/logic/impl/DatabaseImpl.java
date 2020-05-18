package ru.andrey.kvstorage.server.logic.impl;

import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.index.impl.TableIndexImpl;
import ru.andrey.kvstorage.server.initialization.DatabaseInitializationContext;
import ru.andrey.kvstorage.server.logic.Database;
import ru.andrey.kvstorage.server.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DatabaseImpl implements Database {
    private final String dbName;
    private final Path databasePath;
    private final Map<String, Table> tables;

    // todo sukhoa this class is very difficult to test. Think of some proxy/middleware DatabaseInitializer class
    private DatabaseImpl(String dbName, Path databaseRoot) {
        Objects.requireNonNull(dbName);
        Objects.requireNonNull(databaseRoot);

        this.dbName = dbName;
        this.databasePath = databaseRoot.resolve(dbName);
        this.tables = new HashMap<>(16);
    }

    private DatabaseImpl(DatabaseInitializationContext context) {
        this.dbName = context.getDbName();
        this.databasePath = context.getDatabasePath();
        this.tables = context.getTables();
    }

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        DatabaseImpl db = new DatabaseImpl(dbName, databaseRoot);
        db.initializeAsNew();
        return db;
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        return new DatabaseImpl(context);
    }

    private void initializeAsNew() throws DatabaseException {
        if (Files.exists(databasePath)) { // todo sukhoa race condition
            throw new DatabaseException("Database with such name already exists: " + dbName);
        }

        try {
            Files.createDirectory(databasePath);
        } catch (IOException e) {
            throw new DatabaseException("Cannot create database directory for path: " + databasePath, e);
        }
    }

    @Override
    public String getName() {
        return dbName;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        Table table = TableImpl.create(tableName, databasePath, new TableIndexImpl()); // todo sukhoa is this injection ok?
        tables.put(table.getName(), table);
    }

    @Override
    public void createTableIfNotExists(String tableName, int segmentSizeInBytes) throws DatabaseException {

    }

    @Override
    public void write(String tableName, String objectKey, String objectValue) throws DatabaseException {
        Table table = tables.get(tableName);
        if (table == null) {
            throw new DatabaseException("There is no such table: " + tableName);
        }

        table.write(objectKey, objectValue);
    }

    @Override
    public String read(String tableName, String objectKey) throws DatabaseException {
        Table table = tables.get(tableName);
        if (table == null) {
            throw new DatabaseException("There is no such table: " + tableName);
        }

        return table.read(objectKey);
    }
}
