package ru.andrey.kvstorage.server.logic.impl;

import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.logic.Database;
import ru.andrey.kvstorage.server.logic.Segment;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Optional;

import static org.junit.Assert.*;

public class SegmentTest {

    private final String dbName = "db1";
    private final String tableName = "table1";
    private Path dbPath;
    private Database database;

    @Before
    public void setUp() throws IOException, DatabaseException {
        dbPath = Files.createTempDirectory(dbName);
        database = DatabaseImpl.create(dbName, dbPath);
        database.createTableIfNotExists(tableName);
    }

    @After
    public void tearDown() {
        try {
            Files.delete(dbPath);
        } catch (Exception ignored) {}
    }

    @Test
    public void whenSegmentOverflow_createNew() throws NoSuchAlgorithmException, DatabaseException, IOException {
        byte[] array = new byte[50000];
        SecureRandom.getInstanceStrong().nextBytes(array);
        database.write(tableName, "key1", array);
        String segmentsPath = dbPath + "/" + tableName;
        assertEquals(1, Files.list(Path.of(segmentsPath)).count());
        database.write(tableName, "key2", array);
        assertEquals(2, Files.list(Path.of(segmentsPath)).count());
    }

    @Test
    public void create_whenCreated_returnValidName() throws DatabaseException {
        Segment segment = SegmentImpl.create(tableName, Path.of(dbPath + "/" + tableName));
        assertTrue(segment.getName().matches(tableName + "_[0-9]*"));
    }

    @Test
    public void writeRead_WhenValidData_writeAndReadEqualData() throws DatabaseException, IOException {
        Segment segment = SegmentImpl.create(tableName, Path.of(dbPath + "/" + tableName));

        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";

        byte[] data1 = "data1".getBytes(StandardCharsets.UTF_8);
        byte[] data2 = "SomeVeryVeryVeryVeryVeryVeryVeryLongData".getBytes(StandardCharsets.UTF_8);
        byte[] data3 = new byte[] {0, 1, 2, 3, 4, 5};

        segment.write(key1, data1);
        segment.write(key2, data2);
        segment.write(key3, data3);

        Optional<byte[]> actualData1 = segment.read(key1);
        Optional<byte[]> actualData2 = segment.read(key2);
        Optional<byte[]> actualData3 = segment.read(key3);

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
    public void writeRead_WhenNull_ReturnEmptyOptional() throws DatabaseException, IOException {
        Segment segment = SegmentImpl.create(tableName, Path.of(dbPath + "/" + tableName));
        String key = "key1";
        segment.write(key, null);
        assertTrue("Written null value but data read was not null", segment.read(key).isEmpty());
    }

    @Test
    public void read_WhenWasNotWritten_ReturnEmptyOptional() throws DatabaseException, IOException {
        Segment segment = SegmentImpl.create(tableName, Path.of(dbPath + "/" + tableName));
        String key = "key1";
        assertTrue("Written null value but data read was not null", segment.read(key).isEmpty());
    }
}
