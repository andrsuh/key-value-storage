package ru.andrey.kvstorage.server.logic.impl;

import lombok.extern.slf4j.Slf4j;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.index.impl.TableIndex;
import ru.andrey.kvstorage.server.initialization.DatabaseInitializationContext;
import ru.andrey.kvstorage.server.logic.Database;
import ru.andrey.kvstorage.server.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Slf4j
public class DatabaseImpl implements Database {
    private static final int INITIAL_TABLES_CAPACITY = 16;
    private final String dbName;
    private final Path databasePath;
    private final Map<String, Table> tables;

    // todo sukhoa this class is very difficult to test. Think of some proxy/middleware DatabaseInitializer class
    private DatabaseImpl(String dbName, Path databaseRoot) {
        Objects.requireNonNull(dbName);
        Objects.requireNonNull(databaseRoot);

        this.dbName = dbName;
        this.databasePath = databaseRoot.resolve(dbName);
        this.tables = new HashMap<>(INITIAL_TABLES_CAPACITY);
    }

    private DatabaseImpl(DatabaseInitializationContext context) {
        this.dbName = context.getDbName();
        this.databasePath = context.getDatabasePath();
        this.tables = context.getTables();
    }

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        log.info("Creating database with name: {} and root path: {}", dbName, databaseRoot);
        DatabaseImpl db = new DatabaseImpl(dbName, databaseRoot);
        log.debug("Starting of initialization for a new database with name: {}, and path: {}", dbName, databaseRoot);
        db.initializeAsNew();
        log.debug("The database with the name: {} and the path: {} was successful initialized", dbName, databaseRoot);
        return db;
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        return new DatabaseImpl(context);
    }

    private void initializeAsNew() throws DatabaseException {
        if (Files.exists(databasePath)) { // todo sukhoa race condition
            log.error("Database with such path: {} already exists. Database name: {}", databasePath, dbName);
            throw new DatabaseException("Database with such name already exists: " + dbName);
        }

        try {
            log.debug("Trying to create dir with path: {}", databasePath);
            Files.createDirectory(databasePath);
            log.debug("Dir with path: {} successful created", databasePath);
        } catch (IOException e) {
            log.error("Error while creating database with path: {} and name: {} . Error: {}", databasePath, dbName, e.getMessage());
            throw new DatabaseException("Cannot create database directory for path: " + databasePath, e);
        }
    }

    @Override
    public String getName() {
        return dbName;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        log.info("Creating new table {} for database {}", tableName, dbName);
        Table table = TableImpl.create(tableName, databasePath, new TableIndex()); // todo sukhoa is this injection ok?
        tables.put(table.getName(), table);
    }

    @Override
    public void createTableIfNotExists(String tableName, int segmentSizeInBytes) throws DatabaseException {

    }

    @Override
    public void write(String tableName, String objectKey, String objectValue) throws DatabaseException {
        log.info("Writing in table: {}, db name: {}. Key {}, value {}", tableName, dbName, objectKey, objectValue);
        Table table = tables.get(tableName);
        if (table == null) {
            log.warn("No such table with the name {} in the database {}", tableName, dbName);
            throw new DatabaseException("There is no such table: " + tableName);
        }

        log.debug("Trying to write info in the table {}, database {}. Key {}, value {}", tableName, dbName, objectKey, objectValue);
        table.write(objectKey, objectValue);
    }

    @Override
    public Optional<String> read(String tableName, String objectKey) throws DatabaseException {
        log.info("Reading from table {} in database {}. Key {}", tableName, dbName, objectKey);
        Table table = tables.get(tableName);
        if (table == null) {
            log.warn("No such table with the name {} in the database {}", tableName, dbName);
            throw new DatabaseException("There is no such table: " + tableName);
        }

        log.debug("Trying to read info from the table {}, database {}. Key {}", tableName, dbName, objectKey);
        return table.read(objectKey);
    }
}
