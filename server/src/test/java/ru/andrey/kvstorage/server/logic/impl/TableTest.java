package ru.andrey.kvstorage.server.logic.impl;

import org.junit.Before;
import org.junit.Test;
import ru.andrey.kvstorage.server.exception.DatabaseException;
import ru.andrey.kvstorage.server.index.impl.TableIndex;
import ru.andrey.kvstorage.server.logic.Table;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Optional;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class TableTest {

    private Table table;

    @Before
    public void setUp() throws IOException, DatabaseException {
        table = TableImpl.create("table1",
                Files.createTempDirectory("table1"),
                new TableIndex());
    }

    @Test
    public void writeRead_WhenValidData_WriteAndReadEqualData() throws DatabaseException {
        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";

        byte[] data1 = "data1".getBytes(StandardCharsets.UTF_8);
        byte[] data2 = "SomeVeryVeryVeryVeryVeryVeryVeryLongData".getBytes(StandardCharsets.UTF_8);
        byte[] data3 = new byte[] {0, 1, 2, 3, 4, 5};

        table.write(key1, data1);
        table.write(key2, data2);
        table.write(key3, data3);

        Optional<byte[]> actualData1 = table.read(key1);
        Optional<byte[]> actualData2 = table.read(key2);
        Optional<byte[]> actualData3 = table.read(key3);

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
    public void writeReadBugValue() throws NoSuchAlgorithmException, DatabaseException {
        String key = "key1";
        byte[] bigObject = new byte[200000];
        SecureRandom.getInstanceStrong().nextBytes(bigObject);
        table.write("key", bigObject);
        assertArrayEquals(bigObject, table.read("key").get());
    }

    @Test
    public void writeRead_WhenNull_ReturnEmptyOptional() throws DatabaseException {
        String key = "key1";
        table.write(key, null);
        assertTrue("Written null value but data read was not null", table.read(key).isEmpty());
    }

    @Test
    public void read_WhenWasNotWritten_ReturnEmptyOptional() throws DatabaseException {
        String key = "key1";
        assertTrue("Written null value but data read was not null", table.read(key).isEmpty());
    }
}