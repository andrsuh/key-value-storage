package ru.andrey.kvstorage.server.logic;

import org.junit.Before;
import org.junit.Test;
import ru.andrey.kvstorage.server.exceptions.DatabaseException;
import ru.andrey.kvstorage.server.index.impl.TableIndex;
import ru.andrey.kvstorage.server.logic.impl.TableImpl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Optional;
import java.util.Random;

import static org.junit.Assert.*;

public class TableTest {

    private Table table;
    private static final String tableName = "table1";

    @Before
    public void setUp() throws IOException, DatabaseException {
        table = TableImpl.create(tableName,
                Files.createTempDirectory(tableName),
                new TableIndex());
    }

    @Test
    public void getName_ReturnValidName() {
        assertEquals(tableName, table.getName());
    }

    @Test
    public void writeRead_WhenValidData_WriteAndReadEqualData() throws DatabaseException {
        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";

        byte[] data1 = "data1".getBytes(StandardCharsets.UTF_8);
        byte[] data2 = "SomeVeryVeryVeryVeryVeryVeryVeryLongData".getBytes(StandardCharsets.UTF_8);
        byte[] data3 = new byte[]{0, 1, 2, 3, 4, 5};

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
    public void writeRead_WhenBigValue_ThenHandleCorrectly() throws DatabaseException {
        byte[] bigObject = new byte[200000];
        new Random().nextBytes(bigObject);
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

    @Test
    public void writeValue_ThenDelete_HandleCorrectly() throws DatabaseException, IOException {
        String key = "key1";
        byte[] data1 = "data1".getBytes(StandardCharsets.UTF_8);

        table.write(key, data1);

        Optional<byte[]> actualData1 = table.read(key);

        String assertTrueMsg = "Data was written but no data read";
        assertTrue(assertTrueMsg, actualData1.isPresent());

        String assertArrayEqualsMsg = "Data written & data read are not equal";
        assertArrayEquals(assertArrayEqualsMsg, data1, actualData1.get());

        table.delete(key);
        assertTrue(table.read(key).isEmpty());
    }
}