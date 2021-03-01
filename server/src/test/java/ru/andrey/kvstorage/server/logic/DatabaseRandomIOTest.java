package ru.andrey.kvstorage.server.logic;

import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.logic.impl.DatabaseImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class DatabaseRandomIOTest {

    private static final int OPERATIONS_COUNT = 10_000;

    private static final List<String> tables = List.of("table1", "table2", "table3");

    private Path dbPath;
    private Database database;

    private Map<String, Map<String, byte[]>> dataWritten = new HashMap<>();
    private long idx = 0;

    @Before
    public void setUp() throws IOException, DatabaseException {
        dbPath = Files.createTempDirectory("testdb");
        database = DatabaseImpl.create("testdb", dbPath);
        for (String table : tables) {
            database.createTableIfNotExists(table);
            dataWritten.put(table, new HashMap<>());
        }
    }

    @After
    public void tearDown() throws IOException {
        Files.walk(dbPath)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        Files.deleteIfExists(dbPath);
    }

    @Test
    public void writeRead_WhenRandomIO_WriteAndReadValidData() throws DatabaseException {
        for (int i = 0; i < 10; i++)
            testWriteNew();
        for (int i = 0; i < OPERATIONS_COUNT; i++) {
            switch (getRandomOperation()) {
                case READ_VALID:
                    testReadValid();
                    break;
                case READ_INVALID:
                    testReadInvalid();
                    break;
                case WRITE_NEW:
                    testWriteNew();
                    break;
                case WRITE_EXISTING:
                    testWriteExisting();
                    break;
            }
        }
    }

    private void testReadValid() throws DatabaseException {
        String table = getRandomTable();
        String key = getRandomKey(table);
        Optional<byte[]> data = database.read(table, key);
        assertTrue("Read by valid key returned empty optional", data.isPresent());
        assertArrayEquals(dataWritten.get(table).get(key), data.get());
    }

    private void testReadInvalid() throws DatabaseException {
        String table = getRandomTable();
        String key = getRandomKey(table) + "_INVALID";
        Optional<byte[]> data = database.read(table, key);
        assertTrue("Read by invalid key returned non-empty optional", data.isEmpty());
    }

    private void testWriteNew() throws DatabaseException {
        String table = getRandomTable();
        String key = generateKey();
        byte[] value = generateValue();
        database.write(table, key, value);
        dataWritten.get(table).put(key, value);
    }

    private void testWriteExisting() throws DatabaseException {
        String table = getRandomTable();
        String key = getRandomKey(table);
        byte[] value = generateValue();
        dataWritten.get(table).remove(key);
        database.write(table, key, value);
        dataWritten.get(table).put(key, value);
    }

    private String getRandomTable() {
        Random random = new Random();
        return tables.get(random.nextInt(tables.size()));
    }

    private Operation getRandomOperation() {
        Random random = new Random();
        switch (random.nextInt(4)) {
            case 0:
                return Operation.READ_VALID;
            case 1:
                return Operation.READ_INVALID;
            case 2:
                return Operation.WRITE_NEW;
            case 3:
            default:
                return Operation.WRITE_EXISTING;
        }
    }

    private String getRandomKey(String table) {
        List<String> keys = new ArrayList<>(dataWritten.get(table).keySet());
        Random random = new Random();
        return keys.get(random.nextInt(keys.size()));
    }

    private String generateKey() {
        return "somekey" + idx++;
    }

    private byte[] generateValue() {
        Random random = new Random();
        int size = random.nextInt(10000);
        byte[] value = new byte[size];
        random.nextBytes(value);
        return value;
    }

    private enum Operation {
        READ_VALID,
        READ_INVALID,
        WRITE_NEW,
        WRITE_EXISTING
    }
}
