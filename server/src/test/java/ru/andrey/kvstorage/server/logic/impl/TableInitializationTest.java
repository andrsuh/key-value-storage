package ru.andrey.kvstorage.server.logic.impl;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import ru.andrey.kvstorage.server.exceptions.DatabaseException;
import ru.andrey.kvstorage.server.index.impl.TableIndex;
import ru.andrey.kvstorage.server.initialization.impl.TableInitializationContextImpl;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TableInitializationTest {

    private static final String dbName = "db1";
    private static final String tableName = "table1";
    @Rule
    public TemporaryFolder dbRootFolder = new TemporaryFolder();

    private final Random random = new Random();


    @Test
    public void initializeFromContext_WhenEmptyData_CreateEmptyTable() throws IOException {
        Path dbPath = dbRootFolder.newFolder(dbName).toPath();
        dbPath.resolve(tableName).toFile().mkdirs();
        var context = new TableInitializationContextImpl(tableName, dbPath, new TableIndex());
        var table = TableImpl.initializeFromContext(context);
        assertEquals("Name is not valid", tableName, table.getName());
    }

    @Test
    public void initializeFromContext_WhenOneSegment_CreateTableWithData() throws DatabaseException, IOException {
        Path dbPath = dbRootFolder.newFolder(dbName).toPath();

        var index = new TableIndex();
        var newTable = TableImpl.create(tableName, dbPath, index);

        var key1 = "key1";
        var key2 = "key2";
        var key3 = "key3";

        byte[] val1 = new byte[] {1, 2, 3};
        byte[] val2 = new byte[] {4, 5, 6};
        byte[] val3 = new byte[] {7, 8, 9};

        newTable.write(key1, val1);
        newTable.write(key2, val2);
        newTable.write(key3, val3);

        var context = new TableInitializationContextImpl(tableName, dbPath, index);
        var table = TableImpl.initializeFromContext(context);
        assertEquals("Name is not valid", tableName, table.getName());
        assertArrayEquals("Values are not equal", val1, table.read(key1).orElseThrow());
        assertArrayEquals("Values are not equal", val2, table.read(key2).orElseThrow());
        assertArrayEquals("Values are not equal", val3, table.read(key3).orElseThrow());
    }

    @Test
    public void initializeFromContext_WhenMultipleSegments_CreateTableWithData() throws DatabaseException, IOException {
        Path dbPath = dbRootFolder.newFolder(dbName).toPath();

        var index = new TableIndex();
        var newTable = TableImpl.create(tableName, dbPath, index);

        var keys = IntStream.range(0, 20)
                .mapToObj(i -> "key" + i)
                .collect(Collectors.toList());
        var values = IntStream.range(0, keys.size())
                .mapToObj(i -> randomData(20_000))
                .collect(Collectors.toList());

        for (int i = 0; i < keys.size(); i++) {
            newTable.write(keys.get(0), values.get(0));
        }

        var context = new TableInitializationContextImpl(tableName, dbPath, index);
        var table = TableImpl.initializeFromContext(context);
        assertEquals("Name is not valid", tableName, table.getName());
        for (int i = 0; i < keys.size(); i++) {
            assertArrayEquals(
                    "Values are not equal",
                    values.get(0),
                    table.read(keys.get(0)).orElseThrow());
        }
    }

    private byte[] randomData(int size) {
        byte[] data = new byte[size];
        random.nextBytes(data);
        return data;
    }
}
