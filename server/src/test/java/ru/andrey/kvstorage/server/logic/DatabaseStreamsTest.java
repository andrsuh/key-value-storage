package ru.andrey.kvstorage.server.logic;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.andrey.kvstorage.server.logic.DatabaseRecord;
import ru.andrey.kvstorage.server.logic.WritableDatabaseRecord;
import ru.andrey.kvstorage.server.logic.io.DatabaseInputStream;
import ru.andrey.kvstorage.server.logic.io.DatabaseOutputStream;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.junit.Assert.*;

public class DatabaseStreamsTest {

    private DatabaseInputStream inputStream;
    private DatabaseOutputStream outputStream;
    private FileInputStream fileInputStream;
    private FileOutputStream fileOutputStream;
    private File testFile;
    private final String key = "key";
    private final String value = "value";
    private final WritableDatabaseRecord simpleDatabaseRecord =
            new TestRecord(key, "value".getBytes(StandardCharsets.UTF_8));

    @Before
    public void setUp() throws IOException {
        testFile = new File("testfile");
        if (!testFile.createNewFile()) {
            throw new IOException("Cannot create test file");
        }
        fileOutputStream = new FileOutputStream(testFile);
        fileInputStream = new FileInputStream(testFile);
        inputStream = new DatabaseInputStream(fileInputStream);
        outputStream = new DatabaseOutputStream(fileOutputStream);
    }

    @After
    public void tearDown() throws IOException {
        if (!testFile.delete())
            throw new IOException("Cannot delete test file");
    }

    @Test
    public void write_thenCompareRecords() throws IOException {
        byte[] simpleKeyValue = ByteBuffer.allocate(16)
                .putInt(3)
                .put(key.getBytes(StandardCharsets.UTF_8))
                .putInt(5)
                .put(value.getBytes(StandardCharsets.UTF_8))
                .array();
        outputStream.write(simpleDatabaseRecord);
        byte[] bytes = fileInputStream.readAllBytes();
        assertArrayEquals(simpleKeyValue, bytes);
    }

    @Test
    public void read_thenReturnValidRecord() throws IOException {
        byte[] simpleKeyValue = ByteBuffer.allocate(16)
                .putInt(3)
                .put(key.getBytes(StandardCharsets.UTF_8))
                .putInt(5)
                .put(value.getBytes(StandardCharsets.UTF_8))
                .array();
        fileOutputStream.write(simpleKeyValue);
        Optional<DatabaseRecord> record = inputStream.readDbUnit();
        assertTrue(record.isPresent());
        assertTrue(record.get().isValuePresented());
        assertArrayEquals(simpleDatabaseRecord.getKey(), record.get().getKey());
        assertArrayEquals(simpleDatabaseRecord.getValue(), record.get().getValue());
    }

    @Test
    public void writeRead_thenCompareRecords() throws IOException {
        outputStream.write(simpleDatabaseRecord);
        Optional<DatabaseRecord> record = inputStream.readDbUnit();
        assertTrue(record.isPresent());
        assertTrue(record.get().isValuePresented());
        assertArrayEquals(simpleDatabaseRecord.getKey(), record.get().getKey());
        assertArrayEquals(simpleDatabaseRecord.getValue(), record.get().getValue());
    }

    @Test
    public void write_thenReturnValidComplexRecord() throws IOException {
        String value = "I can go with the flow " +
                "Don't say it doesn't matter (With the flow) " +
                "Matter anymore I can go with the flow (I can go) " +
                "Do you believe it in your head?";
        WritableDatabaseRecord record = new TestRecord(key, value.getBytes(StandardCharsets.UTF_8));
        byte[] bytes = ByteBuffer.allocate(11 + value.length())
                .putInt(3)
                .put(key.getBytes(StandardCharsets.UTF_8))
                .putInt(value.length())
                .put(value.getBytes(StandardCharsets.UTF_8))
                .array();
        outputStream.write(record);
        assertArrayEquals(bytes, fileInputStream.readAllBytes());
    }

    @Test
    public void read_thenReturnValidComplexRecord() throws IOException {
        String value = "I can go with the flow " +
                "Don't say it doesn't matter (With the flow) " +
                "Matter anymore I can go with the flow (I can go) " +
                "Do you believe it in your head?";
        byte[] complexKeyValue = ByteBuffer.allocate(11 + value.length())
                .putInt(3)
                .put(key.getBytes(StandardCharsets.UTF_8))
                .putInt(value.length())
                .put(value.getBytes(StandardCharsets.UTF_8))
                .array();
        fileOutputStream.write(complexKeyValue);
        WritableDatabaseRecord expectedRecord = new TestRecord(key, value.getBytes(StandardCharsets.UTF_8));
        Optional<DatabaseRecord> actualRecord = inputStream.readDbUnit();
        assertTrue(actualRecord.isPresent());
        assertTrue(actualRecord.get().isValuePresented());
        assertArrayEquals(expectedRecord.getKey(), actualRecord.get().getKey());
        assertArrayEquals(expectedRecord.getValue(), actualRecord.get().getValue());
    }

    private static class TestRecord implements WritableDatabaseRecord {
        private final byte[] key;
        private final byte[] value;

        public TestRecord(String key, byte[] value) {
            this.key = key.getBytes(StandardCharsets.UTF_8);
            this.value = value;
        }

        @Override
        public byte[] getKey() {
            return key;
        }

        @Override
        public byte[] getValue() {
            return value;
        }

        @Override
        public long size() {
            return 8 + key.length + value.length;
        }

        @Override
        public boolean isValuePresented() {
            return value.length != 0;
        }

        @Override
        public int getKeySize() {
            return key.length;
        }

        @Override
        public int getValueSize() {
            return value.length;
        }
    }
}
