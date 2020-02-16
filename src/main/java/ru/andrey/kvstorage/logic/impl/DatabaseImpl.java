package ru.andrey.kvstorage.logic.impl;

import ru.andrey.kvstorage.exception.DatabaseException;
import ru.andrey.kvstorage.logic.Database;
import ru.andrey.kvstorage.logic.Table;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class DatabaseImpl implements Database {
    private final String dbName;
    private final Path databasePath;
    private final Map<String, Table> tables = new HashMap<>(16);

    // todo sukhoa this class is very difficult to test. Think of some proxy/middleware DatabaseInitializer class
    private DatabaseImpl(String dbName, Path databaseRoot) {
        Objects.requireNonNull(dbName);
        Objects.requireNonNull(databaseRoot);

        this.dbName = dbName;
        this.databasePath = databaseRoot.resolve(dbName);
    }

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        DatabaseImpl db = new DatabaseImpl(dbName, databaseRoot);
        db.initializeAsNew();
        return db;
    }

    public static Database existing(String dbName, Path databaseRoot) throws DatabaseException {
        DatabaseImpl db = new DatabaseImpl(dbName, databaseRoot);
        db.initializeAsExisting();
        return db;
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

    private void initializeAsExisting() throws DatabaseException {
        System.out.println("Creating new database: " + dbName);

        if (!Files.exists(databasePath)) { // todo sukhoa race condition
            throw new DatabaseException("Database with such name doesn't exist: " + dbName);
        }

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(databasePath, p -> Files.isDirectory(p));
             Stream<Path> directoryStream = StreamSupport.stream(ds.spliterator(), false)) {

            directoryStream
                    .forEach(d -> {
                        try {
                            Table table = TableImpl.existing(d.getFileName().toString(), databasePath);
                            tables.put(table.getName(), table);
                        } catch (DatabaseException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (Exception e) {
            throw new DatabaseException("Cannot initialize database: " + dbName, e);
        }
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        Table table = TableImpl.create(tableName, databasePath);
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
