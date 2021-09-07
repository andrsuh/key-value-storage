package ru.andrey.kvstorage.server.logic;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import ru.andrey.kvstorage.server.exceptions.DatabaseException;
import ru.andrey.kvstorage.server.logic.impl.DatabaseImpl;
import ru.andrey.kvstorage.server.logic.impl.SegmentImpl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Random;

import static org.junit.Assert.*;

public class SegmentTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    private final String dbName = "db1";
    private final String tableName = "table1";
    private Path tablePath;
    private Database database;

    @Before
    public void setUp() throws DatabaseException {
        Path dbPath = temporaryFolder.getRoot().toPath();
        database = DatabaseImpl.create(dbName, dbPath);
        database.createTableIfNotExists(tableName);
        tablePath = Path.of(dbPath.toString(), dbName, tableName);
    }

    @Test
    public void create_whenCreated_returnValidName() throws DatabaseException {
        Segment segment = SegmentImpl.create(tableName, tablePath);
        assertTrue(segment.getName().matches(tableName));
    }

    @Test
    public void write_thenAssertReadOnly() throws DatabaseException,IOException {
        Segment segment = SegmentImpl.create(tableName, tablePath);
        byte[] bytes = new byte[1000];
        String keyPrefix = "k";
        int i;
        assertFalse(segment.isReadOnly());
        Random random = new Random();
        for (i = 0; i < 100 && !segment.isReadOnly(); i++) {
            random.nextBytes(bytes);
            if (!segment.write(keyPrefix + i, bytes)) {
                if (i < 97)
                    fail();
                break;
            }
        }
        assertTrue(segment.isReadOnly());
        assertFalse(segment.write("key", bytes));
    }

    @Test
    public void writeRead_WhenValidData_writeAndReadEqualData() throws DatabaseException, IOException {
        Segment segment = SegmentImpl.create(tableName, tablePath);

        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";

        byte[] data1 = "data1".getBytes(StandardCharsets.UTF_8);
        byte[] data2 = "SomeVeryVeryVeryVeryVeryVeryVeryLongData".getBytes(StandardCharsets.UTF_8);
        byte[] data3 = new byte[]{0, 1, 2, 3, 4, 5};

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
    public void whenSegmentOverflow_createNew() throws  DatabaseException, IOException {
        byte[] array = new byte[99988];
        Random random = new Random();
        random.nextBytes(array);
        database.write(tableName, "key1", array);
        assertEquals(1, Files.list(tablePath).count());
        array = new byte[5];
        random.nextBytes(array);
        database.write(tableName, "key2", array);
        assertEquals(2, Files.list(tablePath).count());
    }

    @Test
    public void writeRead_WhenNull_ReturnEmptyOptional() throws DatabaseException, IOException {
        Segment segment = SegmentImpl.create(tableName, tablePath);
        String key = "key1";
        segment.write(key, null);
        assertTrue("Written null value but data read was not null", segment.read(key).isEmpty());
    }

    @Test
    public void read_WhenWasNotWritten_ReturnEmptyOptional() throws DatabaseException, IOException {
        Segment segment = SegmentImpl.create(tableName, tablePath);
        String key = "key1";
        assertTrue("Written null value but data read was not null", segment.read(key).isEmpty());
    }

    @Test
    public void writeRead_WhenBigValue_ThenHandleCorrectly() throws DatabaseException, IOException{
        Segment segment = SegmentImpl.create(tableName, tablePath);
        byte[] bigObject = new byte[200000];
        new Random().nextBytes(bigObject);
        segment.write("key", bigObject);
        assertArrayEquals(bigObject, segment.read("key").get());
    }

    @Test
    public void writeValue_ThenDelete_HandleCorrectly() throws DatabaseException, IOException {
        Segment segment = SegmentImpl.create(tableName, tablePath);
        String key = "key1";
        byte[] data1 = "data1".getBytes(StandardCharsets.UTF_8);

        segment.write(key, data1);

        Optional<byte[]> actualData1 = segment.read(key);

        String assertTrueMsg = "Data was written but no data read";
        assertTrue(assertTrueMsg, actualData1.isPresent());

        String assertArrayEqualsMsg = "Data written & data read are not equal";
        assertArrayEquals(assertArrayEqualsMsg, data1, actualData1.get());

        segment.delete(key);
        assertTrue(segment.read(key).isEmpty());
    }
}
