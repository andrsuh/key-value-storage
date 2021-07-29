package ru.andrey.kvstorage.server.logic.impl;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import ru.andrey.kvstorage.server.exceptions.DatabaseException;
import ru.andrey.kvstorage.server.index.impl.SegmentIndex;
import ru.andrey.kvstorage.server.index.impl.SegmentOffsetInfoImpl;
import ru.andrey.kvstorage.server.initialization.impl.SegmentInitializationContextImpl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.Assert.*;

public class SegmentInitializationTest {

    private static final String dbName = "db1";
    private static final String tableName = "table1";
    @Rule
    public final TemporaryFolder dbFolder = new TemporaryFolder();

    @Test
    public void initializeFromContext_WhenEmptyData_CreateEmptySegment() throws IOException {
        Path tablePath = dbFolder.newFolder(tableName).toPath();
        var name = SegmentImpl.createSegmentName(tableName);
        var path = tablePath.resolve(name);
        boolean created = tablePath.resolve(name).toFile().createNewFile();
        assertTrue(created);
        var context = new SegmentInitializationContextImpl(name, path, 0, new SegmentIndex());
        var segment = SegmentImpl.initializeFromContext(context);
        assertEquals("Segment name is invalid", name, segment.getName());
        assertFalse("Segment is read-only", segment.isReadOnly());
        assertTrue("Cannot write data to segment", segment.write("test", new byte[] {1, 2, 3}));
    }

    @Test
    public void initializeFromContext_WhenContainsData_CreateSegmentWithData() throws IOException, DatabaseException {
        Path tablePath = dbFolder.newFolder(tableName).toPath();

        int segmentSize = 0;
        var index = new SegmentIndex();

        int key1Offset = segmentSize;
        String key1 = "key1";
        segmentSize += 4 + key1.getBytes(StandardCharsets.UTF_8).length;
        byte[] val1 = new byte[] {1, 2, 3};
        segmentSize += 4 + val1.length;
        index.onIndexedEntityUpdated(key1, new SegmentOffsetInfoImpl(key1Offset));

        int key2Offset = segmentSize;
        String key2 = "key2";
        segmentSize += 4 + key2.getBytes(StandardCharsets.UTF_8).length;
        byte[] val2 = new byte[] {4, 5, 6};
        segmentSize += 4 + val2.length;
        index.onIndexedEntityUpdated(key2, new SegmentOffsetInfoImpl(key2Offset));

        int key3Offset = segmentSize;
        String key3 = "key3";
        segmentSize += 4 + key3.getBytes(StandardCharsets.UTF_8).length;
        byte[] val3 = new byte[] {7, 8, 9};
        segmentSize += 4 + val3.length;
        index.onIndexedEntityUpdated(key3, new SegmentOffsetInfoImpl(key3Offset));

        var name = SegmentImpl.createSegmentName(tableName);
        var newSegment = SegmentImpl.create(name, tablePath);

        newSegment.write(key1, val1);
        newSegment.write(key2, val2);
        newSegment.write(key3, val3);

        var context = new SegmentInitializationContextImpl(name, tablePath.resolve(newSegment.getName()), segmentSize, index);
        var segment = SegmentImpl.initializeFromContext(context);
        assertEquals("Segment name is invalid", name, segment.getName());
        assertFalse("Segment is read-only", segment.isReadOnly());
        assertArrayEquals(val1, segment.read(key1).orElseThrow(() -> new NullPointerException("Cannot read data that was written")));
        assertArrayEquals(val2, segment.read(key2).orElseThrow(() -> new NullPointerException("Cannot read data that was written")));
        assertArrayEquals(val3, segment.read(key3).orElseThrow(() -> new NullPointerException("Cannot read data that was written")));
        assertTrue("Cannot write data to segment", segment.write("test", new byte[] {1, 2, 3}));
    }

    @Test
    public void initializeFromContext_WhenReadOnly_CreateReadOnlySegment() throws IOException, DatabaseException {
        Path tablePath = dbFolder.newFolder(tableName).toPath();

        var name = SegmentImpl.createSegmentName(tableName);
        var newSegment = SegmentImpl.create(name, tablePath);
        var segmentSize = 0;
        var index = new SegmentIndex();

        int key1Offset = segmentSize;
        var key1 = "key1";
        segmentSize += 4 + key1.getBytes(StandardCharsets.UTF_8).length;
        byte[] val1 = new byte[40000];
        segmentSize += 4 + 40000;
        index.onIndexedEntityUpdated(key1, new SegmentOffsetInfoImpl(key1Offset));

        int key2Offset = segmentSize;
        var key2 = "key2";
        segmentSize += 4 + key2.getBytes(StandardCharsets.UTF_8).length;
        byte[] val2 = new byte[40000];
        segmentSize += 4 + 40000;
        index.onIndexedEntityUpdated(key2, new SegmentOffsetInfoImpl(key2Offset));

        int key3Offset = segmentSize;
        var key3 = "key3";
        segmentSize += 4 + key3.getBytes(StandardCharsets.UTF_8).length;
        byte[] val3 = new byte[40000];
        segmentSize += 4 + 40000;
        index.onIndexedEntityUpdated(key3, new SegmentOffsetInfoImpl(key3Offset));

        Random random = new Random();
        random.nextBytes(val1);
        random.nextBytes(val2);
        random.nextBytes(val3);

        newSegment.write(key1, val1);
        index.onIndexedEntityUpdated(key1, new SegmentOffsetInfoImpl(key1Offset));
        newSegment.write(key2, val2);
        index.onIndexedEntityUpdated(key2, new SegmentOffsetInfoImpl(key2Offset));
        newSegment.write(key3, val3);
        index.onIndexedEntityUpdated(key3, new SegmentOffsetInfoImpl(key3Offset));

        var context = new SegmentInitializationContextImpl(name, tablePath.resolve(newSegment.getName()), segmentSize, index);
        var segment = SegmentImpl.initializeFromContext(context);

        assertEquals("Segment name is invalid", name, segment.getName());
        assertTrue("Segment is not read-only", segment.isReadOnly());
        assertArrayEquals(val1, segment.read(key1).orElseThrow(() -> new NullPointerException("Cannot read data that was written")));
        assertArrayEquals(val2, segment.read(key2).orElseThrow(() -> new NullPointerException("Cannot read data that was written")));
        assertArrayEquals(val3, segment.read(key3).orElseThrow(() -> new NullPointerException("Cannot read data that was written")));
        assertFalse("Can write data to read-only segment", segment.write("test", new byte[] {1, 2, 3}));
    }
}
