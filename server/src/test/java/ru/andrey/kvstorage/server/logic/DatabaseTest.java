package ru.andrey.kvstorage.server.logic;

import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.logic.impl.DatabaseImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Comparator;
import java.util.Optional;
import java.util.Random;

import static org.junit.Assert.*;

public class DatabaseTest {

    private Path dbPath;
    private Database database;

    @Before
    public void setUp() throws IOException, DatabaseException {
        dbPath = Files.createTempDirectory("testdb");
        database = DatabaseImpl.create("testdb", dbPath);
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
    public void create_WhenNameIsNull_ThrowException() throws IOException {
        Path dbPath = Files.createTempDirectory("testdb");
        assertThrows(Throwable.class, () -> DatabaseImpl.create(null, dbPath));
    }

    @Test
    public void create_WhenPathIsNull_ThrowException() {
        assertThrows(Throwable.class, () -> DatabaseImpl.create("testdb", null));
    }

    @Test
    public void getName_WhenCreated_ReturnValidName() {
        assertEquals("testdb", database.getName());
    }

    @Test
    public void createTableIfNotExists_WhenNoTable_CreateTable() throws DatabaseException {
        database.createTableIfNotExists("table1");
    }

    @Test
    public void createTableIfNotExists_WhenAlreadyExists_ThrowException() throws DatabaseException {
        database.createTableIfNotExists("table1");
        assertThrows(DatabaseException.class, () -> database.createTableIfNotExists("table1"));
    }

    @Test
    public void createTableIfNotExists_WhenNameIsNull_ThrowException() {
        assertThrows(Throwable.class, () -> database.createTableIfNotExists(null));
    }

    @Test
    public void writeRead_WhenValidData_WriteAndReadEqualData() throws DatabaseException {
        String table = "table1";

        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";

        byte[] data1 = "data1".getBytes(StandardCharsets.UTF_8);
        byte[] data2 = "SomeVeryVeryVeryVeryVeryVeryVeryLongData".getBytes(StandardCharsets.UTF_8);
        byte[] data3 = new byte[] {0, 1, 2, 3, 4, 5};

        database.createTableIfNotExists(table);

        database.write(table, key1, data1);
        database.write(table, key2, data2);
        database.write(table, key3, data3);

        Optional<byte[]> actualData1 = database.read(table, key1);
        Optional<byte[]> actualData2 = database.read(table, key2);
        Optional<byte[]> actualData3 = database.read(table, key3);

        String assertTrueMsg = "Data was written but no data read";
        assertTrue(assertTrueMsg, actualData1.isPresent());
        assertTrue(assertTrueMsg, actualData2.isPresent());
        assertTrue(assertTrueMsg, actualData3.isPresent());

        String assertArrayEqualsMsg = "Data written & data read are not equal";
        assertArrayEquals(assertArrayEqualsMsg, data1, actualData1.get());
        assertArrayEquals(assertArrayEqualsMsg, data2, actualData2.get());
        assertArrayEquals(assertArrayEqualsMsg, data3, actualData3.get());
    }

    @Test
    public void writeRead_WhenMultipleTables_CorrectlyWriteRead() throws DatabaseException {
        String table1 = "table1";
        String table2 = "table2";
        String table3 = "table3";

        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";

        byte[] data1 = "data1".getBytes(StandardCharsets.UTF_8);
        byte[] data2 = "SomeVeryVeryVeryVeryVeryVeryVeryLongData".getBytes(StandardCharsets.UTF_8);
        byte[] data3 = new byte[] {0, 1, 2, 3, 4, 5};

        database.createTableIfNotExists(table1);
        database.createTableIfNotExists(table2);
        database.createTableIfNotExists(table3);

        database.write(table1, key1, data1);
        database.write(table2, key2, data2);
        database.write(table3, key3, data3);

        Optional<byte[]> actualData1 = database.read(table1, key1);
        Optional<byte[]> actualData2 = database.read(table2, key2);
        Optional<byte[]> actualData3 = database.read(table3, key3);

        String assertTrueMsg = "Data was written but no data read";
        assertTrue(assertTrueMsg, actualData1.isPresent());
        assertTrue(assertTrueMsg, actualData2.isPresent());
        assertTrue(assertTrueMsg, actualData3.isPresent());

        String assertArrayEqualsMsg = "Data written & data read are not equal";
        assertArrayEquals(assertArrayEqualsMsg, data1, actualData1.get());
        assertArrayEquals(assertArrayEqualsMsg, data2, actualData2.get());
        assertArrayEquals(assertArrayEqualsMsg, data3, actualData3.get());
    }

    @Test
    public void writeRead_WhenVeryLongKey_HandleCorrectly() throws DatabaseException {
        String table = "table1";

        String key1 = "key1".repeat(2500);
        String key2 = "key2".repeat(5000);
        String key3 = "key3".repeat(12500);

        byte[] data1 = "value1".getBytes();
        byte[] data2 = "value2".getBytes();
        byte[] data3 = "value3".getBytes();

        Random random = new Random();

        random.nextBytes(data1);
        random.nextBytes(data2);
        random.nextBytes(data3);

        database.createTableIfNotExists(table);

        database.write(table, key1, data1);
        database.write(table, key2, data2);
        database.write(table, key3, data3);

        Optional<byte[]> actualData1 = database.read(table, key1);
        Optional<byte[]> actualData2 = database.read(table, key2);
        Optional<byte[]> actualData3 = database.read(table, key3);

        String assertTrueMsg = "Data was written but no data read";
        assertTrue(assertTrueMsg, actualData1.isPresent());
        assertTrue(assertTrueMsg, actualData2.isPresent());
        assertTrue(assertTrueMsg, actualData3.isPresent());

        String assertArrayEqualsMsg = "Data written & data read are not equal";
        assertArrayEquals(assertArrayEqualsMsg, data1, actualData1.get());
        assertArrayEquals(assertArrayEqualsMsg, data2, actualData2.get());
        assertArrayEquals(assertArrayEqualsMsg, data3, actualData3.get());
    }

    @Test
    public void writeRead_WhenVeryLongValue_HandleCorrectly() throws DatabaseException {
        String table = "table1";

        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";

        byte[] data1 = new byte[10000];
        byte[] data2 = new byte[20000];
        byte[] data3 = new byte[50000];

        Random random = new Random();

        random.nextBytes(data1);
        random.nextBytes(data2);
        random.nextBytes(data3);

        database.createTableIfNotExists(table);

        database.write(table, key1, data1);
        database.write(table, key2, data2);
        database.write(table, key3, data3);

        Optional<byte[]> actualData1 = database.read(table, key1);
        Optional<byte[]> actualData2 = database.read(table, key2);
        Optional<byte[]> actualData3 = database.read(table, key3);

        String assertTrueMsg = "Data was written but no data read";
        assertTrue(assertTrueMsg, actualData1.isPresent());
        assertTrue(assertTrueMsg, actualData2.isPresent());
        assertTrue(assertTrueMsg, actualData3.isPresent());

        String assertArrayEqualsMsg = "Data written & data read are not equal";
        assertArrayEquals(assertArrayEqualsMsg, data1, actualData1.get());
        assertArrayEquals(assertArrayEqualsMsg, data2, actualData2.get());
        assertArrayEquals(assertArrayEqualsMsg, data3, actualData3.get());
    }

    @Test
    public void writeReadBigValue() throws NoSuchAlgorithmException, DatabaseException {
        byte[] bigObject = new byte[200000];
        SecureRandom.getInstanceStrong().nextBytes(bigObject);
        database.createTableIfNotExists("table");
        database.write("table", "key1", bigObject);
        assertArrayEquals(bigObject, database.read("table", "key1").get());
    }

    @Test
    public void writeRead_WhenNull_ReturnEmptyOptional() throws DatabaseException {
        String table = "table1";
        String key = "key1";
        database.createTableIfNotExists(table);
        database.write(table, key, null);
        assertTrue("Written null value but data read was not null", database.read(table, key).isEmpty());
    }

    @Test
    public void read_WhenWasNotWritten_ReturnEmptyOptional() throws DatabaseException {
        String table = "table1";
        String key = "key1";
        database.createTableIfNotExists(table);
        assertTrue("Written null value but data read was not null", database.read(table, key).isEmpty());
    }

    @Test
    public void write_WhenNoSuchTable_ThrowException() {
        assertThrows(DatabaseException.class,
                () -> database.write("INVALID_NAME", "key1", "data1".getBytes()));
    }
}