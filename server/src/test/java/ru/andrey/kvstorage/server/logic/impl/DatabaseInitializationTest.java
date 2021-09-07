package ru.andrey.kvstorage.server.logic.impl;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import ru.andrey.kvstorage.server.config.DatabaseConfig;
import ru.andrey.kvstorage.server.console.impl.ExecutionEnvironmentImpl;
import ru.andrey.kvstorage.server.exceptions.DatabaseException;
import ru.andrey.kvstorage.server.initialization.impl.*;
import ru.andrey.kvstorage.server.logic.Database;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class DatabaseInitializationTest {

    private static final String dbName = "db1";
    @Rule
    public TemporaryFolder dbRootFolder = new TemporaryFolder();

    private final Random random = new Random();

    @Test
    public void initializeFromContext_WhenEmptyDatabase() {
        var context = new DatabaseInitializationContextImpl(dbName, dbRootFolder.getRoot().toPath());
        var database = DatabaseImpl.initializeFromContext(context);
        assertEquals("Name is not valid", dbName, database.getName());
    }

    @Test
    public void initializeFromContext_WhenDatabaseWithTables() throws DatabaseException {
        Path dbRootPath = dbRootFolder.getRoot().toPath();
        var newDb = DatabaseImpl.create(dbName, dbRootPath);

        String table1 = "table1";
        String table2 = "table2";
        String table3 = "table3";

        List<Map.Entry<String, byte[]>> data1 = IntStream.range(0, 20)
                .mapToObj(i -> "key1_" + i)
                .map(key -> Map.entry(key, randomData(20_000)))
                .collect(Collectors.toList());

        List<Map.Entry<String, byte[]>> data2 = IntStream.range(0, 20)
                .mapToObj(i -> "key2_" + i)
                .map(key -> Map.entry(key, randomData(20_000)))
                .collect(Collectors.toList());

        List<Map.Entry<String, byte[]>> data3 = IntStream.range(0, 20)
                .mapToObj(i -> "key3_" + i)
                .map(key -> Map.entry(key, randomData(20_000)))
                .collect(Collectors.toList());

        newDb.createTableIfNotExists(table1);
        newDb.createTableIfNotExists(table2);
        newDb.createTableIfNotExists(table3);

        data1.forEach(entry -> writeToDb(newDb, table1, entry.getKey(), entry.getValue()));
        data2.forEach(entry -> writeToDb(newDb, table2, entry.getKey(), entry.getValue()));
        data3.forEach(entry -> writeToDb(newDb, table3, entry.getKey(), entry.getValue()));

        var databaseInitializer = new DatabaseInitializer(
                new TableInitializer(
                        new SegmentInitializer()
                )
        );
        var env = new ExecutionEnvironmentImpl(new DatabaseConfig(dbRootPath.toString()));
        var context = new InitializationContextImpl(env, new DatabaseInitializationContextImpl(dbName, dbRootPath), null, null);
        databaseInitializer.perform(context);
        var database = DatabaseImpl.initializeFromContext(context.currentDbContext());

        BiConsumer<String, Map.Entry<String, byte[]>> checker = (table, entry) -> {
            try {
                assertArrayEquals(entry.getValue(), database.read(table, entry.getKey()).orElseThrow());
            } catch (DatabaseException e) {
                throw new RuntimeException(e);
            }
        };

        data1.forEach(entry -> checker.accept(table1, entry));
        data2.forEach(entry -> checker.accept(table2, entry));
        data3.forEach(entry -> checker.accept(table3, entry));
    }

    private byte[] randomData(int size) {
        byte[] data = new byte[size];
        random.nextBytes(data);
        return data;
    }

    private void writeToDb(Database db, String table, String key, byte[] value) {
        try {
            db.write(table, key, value);
        } catch (DatabaseException ex) {
            throw new RuntimeException(ex);
        }
    }
}
